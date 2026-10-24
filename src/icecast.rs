//! Talking to an Icecast server as a source: the handshake (HTTP PUT, as
//! Icecast 2.4 and later take it, or the older SOURCE), and title updates.
//! After the handshake the connection carries the MP3, raw, until it closes.

use std::fmt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::base64;

/// How to start a stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    /// Automatic (PUT, then SOURCE)
    Auto,
    /// PUT (Icecast 2.4 and later)
    Put,
    /// SOURCE (older servers)
    Source,
}

/// The request method a stream was started with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Put,
    Source,
}

/// What listeners are told about the stream.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamInfo {
    pub name: String,
    pub description: String,
    pub genre: String,
    pub url: String,
    pub public: bool,
    pub rate: u32,
    pub kbps: u32,
    pub stereo: bool,
}

/// A mount on a server, and how to stream to it.
#[derive(Clone, PartialEq)]
pub struct Target {
    pub host: String,
    pub port: u16,
    /// With its leading `/`.
    pub mount: String,
    pub user: String,
    pub password: String,
    pub protocol: Protocol,
    pub info: StreamInfo,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}{}", self.host, self.port, self.mount)
    }
}

/// The password is never shown.
impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Target")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("mount", &self.mount)
            .field("user", &self.user)
            .field("password", &"***")
            .field("protocol", &self.protocol)
            .field("info", &self.info)
            .finish()
    }
}

/// How long to wait to connect, and for the server to answer.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Start a stream to `t`. With [`Protocol::Auto`] it tries PUT, then SOURCE if
/// the server doesn't take PUT, and sets `use_source` so later tries go straight to SOURCE.
pub fn open(t: &Target, use_source: &mut bool) -> Result<(TcpStream, Method), String> {
    let method = match t.protocol {
        Protocol::Put => Method::Put,
        Protocol::Source => Method::Source,
        Protocol::Auto if *use_source => Method::Source,
        Protocol::Auto => Method::Put,
    };
    match start(t, method) {
        Err(Refusal::Unsupported(_)) if t.protocol == Protocol::Auto && method == Method::Put => {
            let s = start(t, Method::Source).map_err(Refusal::into_message)?;
            // Only once SOURCE has worked: a server that's down isn't one without PUT.
            *use_source = true;
            Ok((s, Method::Source))
        }
        r => r.map(|s| (s, method)).map_err(Refusal::into_message),
    }
}

/// Set the stream's title (what listeners see as playing).
pub fn update_title(t: &Target, title: &str) -> Result<(), String> {
    let mut s = connect(t)?;
    let head = format!(
        "GET /admin/metadata?mount={}&mode=updinfo&song={} HTTP/1.0\r\nHost: {}:{}\r\nAuthorization: {}\r\nUser-Agent: {}\r\n\r\n",
        urlencode(&t.mount),
        urlencode(title),
        t.host,
        t.port,
        auth(t),
        user_agent()
    );
    s.write_all(head.as_bytes()).map_err(|e| format!("{t}: {e}"))?;
    match response(&mut s) {
        Some((200..=299, _)) => Ok(()),
        Some((code, reason)) => Err(format!("{t} didn't take the title: {code} {reason}")),
        None => Err(format!("{t} didn't answer the title update")),
    }
}

enum Refusal {
    /// The server doesn't take this method: worth trying the other.
    Unsupported(String),
    Failed(String),
}

impl Refusal {
    fn into_message(self) -> String {
        match self {
            Refusal::Unsupported(m) | Refusal::Failed(m) => m,
        }
    }
}

fn start(t: &Target, method: Method) -> Result<TcpStream, Refusal> {
    let mut s = connect(t).map_err(Refusal::Failed)?;
    let i = &t.info;
    let mut head = match method {
        Method::Put => format!("PUT {} HTTP/1.1\r\n", t.mount),
        Method::Source => format!("SOURCE {} HTTP/1.0\r\n", t.mount),
    };
    let mut add = |name: &str, value: &str| head.push_str(&format!("{name}: {}\r\n", one_line(value)));
    add("Host", &format!("{}:{}", t.host, t.port));
    add("Authorization", &auth(t));
    add("User-Agent", &user_agent());
    add("Content-Type", "audio/mpeg");
    if method == Method::Put {
        add("Expect", "100-continue");
    }
    add("ice-public", if i.public { "1" } else { "0" });
    for (name, value) in [("ice-name", &i.name), ("ice-description", &i.description), ("ice-genre", &i.genre), ("ice-url", &i.url)] {
        if !value.trim().is_empty() {
            add(name, value);
        }
    }
    add("ice-bitrate", &i.kbps.to_string());
    add("ice-audio-info", &format!("channels={};samplerate={};bitrate={}", if i.stereo { 2 } else { 1 }, i.rate, i.kbps));
    head.push_str("\r\n");
    s.write_all(head.as_bytes()).map_err(|e| Refusal::Failed(format!("{t}: {e}")))?;
    let unsupported = |why: String| if method == Method::Put { Refusal::Unsupported(why) } else { Refusal::Failed(why) };
    match response(&mut s) {
        Some((100 | 200..=299, _)) => {
            let _ = s.set_read_timeout(None);
            Ok(s)
        }
        Some((401, _)) => Err(Refusal::Failed(format!("{t}: wrong username or password"))),
        Some((403, reason)) => Err(Refusal::Failed(format!("{t}: the server refused the mount (in use by another source, or not allowed): 403 {reason}"))),
        Some((404, _)) => Err(Refusal::Failed(format!("{t}: there's no such mount"))),
        Some((code @ (400 | 405 | 501), reason)) => Err(unsupported(format!("{t}: the server answered {code} {reason}"))),
        Some((code, reason)) => Err(Refusal::Failed(format!("{t}: the server answered {code} {reason}"))),
        None => Err(unsupported(format!("{t}: the server hung up without answering"))),
    }
}

fn connect(t: &Target) -> Result<TcpStream, String> {
    let addrs: Vec<SocketAddr> = (t.host.as_str(), t.port).to_socket_addrs().map_err(|e| format!("can't find {}: {e}", t.host))?.collect();
    let mut last = format!("can't find {}", t.host);
    for a in addrs {
        match TcpStream::connect_timeout(&a, TIMEOUT) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                let _ = s.set_read_timeout(Some(TIMEOUT));
                let _ = s.set_write_timeout(Some(TIMEOUT));
                return Ok(s);
            }
            Err(e) => last = format!("can't connect to {t}: {e}"),
        }
    }
    Err(last)
}

/// The status of the server's answer, from its head (read to its blank line).
fn response(s: &mut TcpStream) -> Option<(u16, String)> {
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") && !head.ends_with(b"\n\n") && head.len() < 8192 {
        match s.read(&mut b) {
            Ok(1) => head.push(b[0]),
            _ => break,
        }
    }
    let head = String::from_utf8_lossy(&head);
    let mut parts = head.lines().next()?.splitn(3, ' ');
    let proto = parts.next()?;
    if !proto.starts_with("HTTP/") && !proto.starts_with("ICE/") {
        return None;
    }
    let code = parts.next()?.parse().ok()?;
    Some((code, parts.next().unwrap_or("").trim().to_string()))
}

fn auth(t: &Target) -> String {
    format!("Basic {}", base64::encode(format!("{}:{}", t.user, t.password).as_bytes()))
}

fn user_agent() -> String {
    format!("icecast-endpoint/{}", env!("CARGO_PKG_VERSION"))
}

/// A header value can't break its line.
fn one_line(v: &str) -> String {
    v.replace("\r\n", " ").replace(['\r', '\n'], " ")
}

/// Percent-encoding for a query string: everything but letters, digits and `-_.~`.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// A stand-in Icecast server for tests: it answers each connection's request
/// with `respond(request head)` and keeps what it's sent.
#[cfg(test)]
pub mod fake {
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[derive(Clone, Debug, Default)]
    pub struct Conn {
        pub head: String,
        pub body: Vec<u8>,
        pub closed: bool,
    }

    type Respond = dyn Fn(&str) -> String + Send + Sync;

    pub struct Server {
        pub port: u16,
        conns: Arc<Mutex<Vec<Conn>>>,
        streams: Arc<Mutex<Vec<TcpStream>>>,
    }

    impl Server {
        /// `respond` gives the whole response head ("HTTP/1.0 200 OK\r\n\r\n"); empty closes without one.
        pub fn start(respond: impl Fn(&str) -> String + Send + Sync + 'static) -> Server {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            let conns: Arc<Mutex<Vec<Conn>>> = Default::default();
            let streams: Arc<Mutex<Vec<TcpStream>>> = Default::default();
            let respond: Arc<Respond> = Arc::new(respond);
            let (c, s) = (conns.clone(), streams.clone());
            std::thread::spawn(move || {
                for stream in l.incoming().flatten() {
                    let (c, s, respond) = (c.clone(), s.clone(), respond.clone());
                    std::thread::spawn(move || serve(stream, &c, &s, &*respond));
                }
            });
            Server { port, conns, streams }
        }

        /// A server that takes any stream.
        pub fn ok() -> Server {
            Server::start(|head| if head.starts_with("PUT") { "HTTP/1.1 100 Continue\r\n\r\n".into() } else { "HTTP/1.0 200 OK\r\n\r\n".into() })
        }

        pub fn conns(&self) -> Vec<Conn> {
            self.conns.lock().unwrap().clone()
        }

        /// Wait (up to 5 s) until `f` holds of the connections so far.
        pub fn wait(&self, f: impl Fn(&[Conn]) -> bool) -> Vec<Conn> {
            let until = Instant::now() + Duration::from_secs(5);
            loop {
                let c = self.conns();
                if f(&c) || Instant::now() > until {
                    return c;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// Hang up on every source.
        pub fn kick(&self) {
            for s in self.streams.lock().unwrap().drain(..) {
                let _ = s.shutdown(Shutdown::Both);
            }
        }
    }

    fn serve(mut s: TcpStream, conns: &Mutex<Vec<Conn>>, streams: &Mutex<Vec<TcpStream>>, respond: &Respond) {
        let mut head = Vec::new();
        let mut b = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match s.read(&mut b) {
                Ok(1) => head.push(b[0]),
                _ => return,
            }
        }
        let head = String::from_utf8_lossy(&head).to_string();
        let i = {
            let mut c = conns.lock().unwrap();
            c.push(Conn { head: head.clone(), ..Default::default() });
            c.len() - 1
        };
        let answer = respond(&head);
        if answer.is_empty() || s.write_all(answer.as_bytes()).is_err() {
            return;
        }
        let streaming = answer.contains(" 100 ") || answer.contains(" 200 ");
        if !streaming {
            return;
        }
        streams.lock().unwrap().push(s.try_clone().unwrap());
        let mut buf = [0u8; 8192];
        loop {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => conns.lock().unwrap()[i].body.extend_from_slice(&buf[..n]),
            }
        }
        conns.lock().unwrap()[i].closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn target(port: u16, protocol: Protocol) -> Target {
        Target {
            host: "127.0.0.1".into(),
            port,
            mount: "/feed".into(),
            user: "source".into(),
            password: "hackme".into(),
            protocol,
            info: StreamInfo {
                name: "County Fire".into(),
                description: "Fire dispatch".into(),
                genre: "Scanner".into(),
                url: "https://example.com".into(),
                public: false,
                rate: 22050,
                kbps: 16,
                stereo: false,
            },
        }
    }

    fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|l| l.split_once(": ").filter(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v))
    }

    #[test]
    fn put_with_the_streams_details() {
        let server = fake::Server::ok();
        let (mut s, method) = open(&target(server.port, Protocol::Auto), &mut false).unwrap();
        assert_eq!(method, Method::Put);
        s.write_all(b"mp3").unwrap();
        drop(s);
        let c = &server.wait(|c| c.first().is_some_and(|c| c.closed))[0];
        assert!(c.head.starts_with("PUT /feed HTTP/1.1\r\n"), "{}", c.head);
        // source:hackme
        assert_eq!(header(&c.head, "Authorization"), Some("Basic c291cmNlOmhhY2ttZQ=="));
        assert_eq!(header(&c.head, "Host"), Some(format!("127.0.0.1:{}", server.port).as_str()));
        assert_eq!(header(&c.head, "Content-Type"), Some("audio/mpeg"));
        assert_eq!(header(&c.head, "Expect"), Some("100-continue"));
        assert_eq!(header(&c.head, "ice-name"), Some("County Fire"));
        assert_eq!(header(&c.head, "ice-description"), Some("Fire dispatch"));
        assert_eq!(header(&c.head, "ice-genre"), Some("Scanner"));
        assert_eq!(header(&c.head, "ice-url"), Some("https://example.com"));
        assert_eq!(header(&c.head, "ice-public"), Some("0"));
        assert_eq!(header(&c.head, "ice-bitrate"), Some("16"));
        assert_eq!(header(&c.head, "ice-audio-info"), Some("channels=1;samplerate=22050;bitrate=16"));
        assert!(header(&c.head, "User-Agent").unwrap().starts_with("icecast-endpoint/"));
        assert_eq!(c.body, b"mp3");
    }

    #[test]
    fn a_plain_200_starts_a_put_too() {
        let server = fake::Server::start(|_| "HTTP/1.0 200 OK\r\n\r\n".into());
        assert_eq!(open(&target(server.port, Protocol::Put), &mut false).unwrap().1, Method::Put);
    }

    #[test]
    fn falls_back_to_source_and_remembers() {
        let server =
            fake::Server::start(
                |head| {
                    if head.starts_with("PUT") { "HTTP/1.0 405 Method Not Allowed\r\n\r\n".into() } else { "HTTP/1.0 200 OK\r\n\r\n".into() }
                },
            );
        let t = target(server.port, Protocol::Auto);
        let mut use_source = false;
        assert_eq!(open(&t, &mut use_source).unwrap().1, Method::Source);
        assert!(use_source);
        assert_eq!(open(&t, &mut use_source).unwrap().1, Method::Source);
        let c = server.wait(|c| c.len() == 3);
        let lines: Vec<&str> = c.iter().map(|c| c.head.lines().next().unwrap()).collect();
        assert_eq!(lines, ["PUT /feed HTTP/1.1", "SOURCE /feed HTTP/1.0", "SOURCE /feed HTTP/1.0"]);
        assert_eq!(header(&c[1].head, "Authorization"), Some("Basic c291cmNlOmhhY2ttZQ=="));
        assert_eq!(header(&c[1].head, "Expect"), None);
    }

    #[test]
    fn a_server_that_hangs_up_on_put_gets_source() {
        let server = fake::Server::start(|head| if head.starts_with("PUT") { String::new() } else { "HTTP/1.0 200 OK\r\n\r\n".into() });
        assert_eq!(open(&target(server.port, Protocol::Auto), &mut false).unwrap().1, Method::Source);
    }

    #[test]
    fn source_when_asked() {
        let server = fake::Server::ok();
        assert_eq!(open(&target(server.port, Protocol::Source), &mut false).unwrap().1, Method::Source);
        assert!(server.wait(|c| !c.is_empty())[0].head.starts_with("SOURCE /feed HTTP/1.0\r\n"));
    }

    #[test]
    fn put_only_when_asked() {
        let server = fake::Server::start(|_| "HTTP/1.0 405 Method Not Allowed\r\n\r\n".into());
        let e = open(&target(server.port, Protocol::Put), &mut false).unwrap_err();
        assert!(e.contains("405"), "{e}");
        assert_eq!(server.conns().len(), 1);
    }

    #[test]
    fn a_wrong_password_says_so_without_it() {
        let server = fake::Server::start(|_| "HTTP/1.0 401 Authentication Required\r\n\r\n".into());
        let e = open(&target(server.port, Protocol::Auto), &mut false).unwrap_err();
        assert!(e.contains("wrong username or password"), "{e}");
        assert!(e.contains(&format!("127.0.0.1:{}/feed", server.port)), "{e}");
        assert!(!e.contains("hackme"));
        // Not a reason to try SOURCE.
        assert_eq!(server.conns().len(), 1);
    }

    #[test]
    fn a_mount_in_use() {
        let server = fake::Server::start(|_| "HTTP/1.0 403 Mountpoint in use\r\n\r\n".into());
        let e = open(&target(server.port, Protocol::Auto), &mut false).unwrap_err();
        assert!(e.contains("in use"), "{e}");
    }

    #[test]
    fn nothing_listening() {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let e = open(&target(port, Protocol::Auto), &mut false).unwrap_err();
        assert!(e.starts_with("can't connect to 127.0.0.1:"), "{e}");
    }

    #[test]
    fn header_values_stay_on_their_line() {
        let server = fake::Server::ok();
        let mut t = target(server.port, Protocol::Put);
        t.info.name = "Evil\r\nice-public: 1".into();
        open(&t, &mut false).unwrap();
        let c = &server.wait(|c| !c.is_empty())[0];
        assert_eq!(header(&c.head, "ice-name"), Some("Evil ice-public: 1"));
        assert_eq!(header(&c.head, "ice-public"), Some("0"));
    }

    #[test]
    fn titles() {
        let server = fake::Server::ok();
        update_title(&target(server.port, Protocol::Auto), "Fire Dispatch (101) & more").unwrap();
        let c = &server.wait(|c| !c.is_empty())[0];
        assert!(c.head.starts_with("GET /admin/metadata?mount=%2Ffeed&mode=updinfo&song=Fire%20Dispatch%20%28101%29%20%26%20more HTTP/1.0\r\n"), "{}", c.head);
        assert_eq!(header(&c.head, "Authorization"), Some("Basic c291cmNlOmhhY2ttZQ=="));
    }

    #[test]
    fn a_refused_title_says_why() {
        let server = fake::Server::start(|_| "HTTP/1.0 403 Forbidden\r\n\r\n".into());
        assert!(update_title(&target(server.port, Protocol::Auto), "x").unwrap_err().contains("403"));
    }

    #[test]
    fn the_password_is_never_shown() {
        let t = target(8000, Protocol::Auto);
        assert!(!format!("{t:?}").contains("hackme"));
        assert!(!t.to_string().contains("hackme"));
        assert_eq!(t.to_string(), "127.0.0.1:8000/feed");
    }
}
