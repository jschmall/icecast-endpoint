//! A feed filled in from a Broadcastify account: log in with the Broadcastify username and
//! password and get the account's feeds, each with its server, port, mount and source password
//! (the API the Broadcastify appliance uses). When the answer can't be had, say why: on a city,
//! county, school or company network it's usually a firewall, a web filter or a proxy, and the
//! user needs to hear that, not "it failed".

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

/// Where an account's feeds come from.
pub const API: &str = "https://api.broadcastify.com/appl/v1/";
/// How long the API has to answer (a button's answer is waited for 15 s in all).
const API_TIMEOUT: Duration = Duration::from_secs(7);
/// How long the feed's server has to take a connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// What to do on a network that blocks the way out.
const ASK_IT: &str = "If this computer is on a city, county, school or company network, ask its IT staff to allow outbound HTTPS (port 443) to api.broadcastify.com, and outbound connections to your feed's Broadcastify server (such as audio9.broadcastify.com, port 80).";

/// One of an account's feeds, as the API gives it.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct BcfyFeed {
    #[serde(rename = "feedId", deserialize_with = "number")]
    pub id: u32,
    #[serde(rename = "feedDescr", default)]
    pub name: String,
    #[serde(default)]
    pub mount: String,
    #[serde(default)]
    pub source_password: String,
    #[serde(rename = "hostname_ext", default)]
    pub host: String,
    #[serde(rename = "port_ext", deserialize_with = "number")]
    pub port: u16,
}

impl BcfyFeed {
    /// Its mount, as the plugin's settings have it (`/abcd1234`).
    pub fn mount(&self) -> String {
        format!("/{}", self.mount.trim().trim_start_matches('/'))
    }
}

/// A number, or a number as a string (as the API sends its database's columns).
fn number<'de, D: serde::Deserializer<'de>, T: std::str::FromStr + TryFrom<u64>>(d: D) -> Result<T, D::Error> {
    match Value::deserialize(d)? {
        Value::Number(n) => n.as_u64().and_then(|n| T::try_from(n).ok()).ok_or_else(|| serde::de::Error::custom("not a number")),
        Value::String(s) => s.trim().parse().map_err(|_| serde::de::Error::custom("not a number")),
        _ => Err(serde::de::Error::custom("not a number")),
    }
}

/// The account's feeds, or what to tell the user.
pub fn feeds(api: &str, username: &str, password: &str) -> Result<Vec<BcfyFeed>, String> {
    let host = host_of(api);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(API_TIMEOUT))
        .http_status_as_error(false)
        .user_agent(concat!("icecast-endpoint/", env!("CARGO_PKG_VERSION")))
        .build()
        .into();
    let mut r = agent.post(api).send_form([("a", "get_user_feeds"), ("u", username), ("p", password)]).map_err(|e| unreachable(&host, &e))?;
    let status = r.status().as_u16();
    let body = r.body_mut().with_config().limit(1 << 20).read_to_string().unwrap_or_default();
    answer(&host, status, &body)
}

/// What an answer says: the feeds, Broadcastify's own error, or something else that answered
/// in its place (a firewall's or web filter's page, a proxy).
pub fn answer(host: &str, status: u16, body: &str) -> Result<Vec<BcfyFeed>, String> {
    if status == 407 {
        return Err(format!("A proxy on your network wants a login before it lets this computer reach {host}. {ASK_IT}"));
    }
    match serde_json::from_str::<Value>(body) {
        Ok(Value::Array(feeds)) if status == 200 => {
            let feeds: Vec<BcfyFeed> = feeds.into_iter().filter_map(|f| serde_json::from_value(f).ok()).collect();
            if feeds.is_empty() { Err("This Broadcastify account has no feeds.".into()) } else { Ok(feeds) }
        }
        Ok(Value::Object(e)) if e.get("error").is_some_and(|v| v == true) => {
            let said = e.get("errorMsg").and_then(Value::as_str).unwrap_or("").trim();
            Err(match said {
                "Invalid Username or Password" => {
                    "Broadcastify didn't accept that username and password. Check them (they're the ones you log in to broadcastify.com with).".into()
                }
                "No username or password sent" => "Type your Broadcastify username and password first.".into(),
                "No feeds available for user" => "This Broadcastify account has no feeds.".into(),
                "" => "Broadcastify turned down the request.".into(),
                s => format!("Broadcastify says: {s}"),
            })
        }
        _ => {
            let what = if status == 200 { "a page that isn't Broadcastify's".to_string() } else { format!("HTTP {status}") };
            let said = page_text(body);
            let said = if said.is_empty() { String::new() } else { format!(" (\"{said}\")") };
            Err(format!(
                "Something on your network answered in place of {host}: {what}{said}. That's usually a firewall or web filter blocking the request. {ASK_IT}"
            ))
        }
    }
}

/// Why a request didn't get to `host`, and what to do.
fn unreachable(host: &str, e: &ureq::Error) -> String {
    use std::io::ErrorKind::*;
    let why = match e {
        ureq::Error::HostNotFound => format!("This computer couldn't look up {host}: it isn't on the internet, or the network's DNS filter blocks the name."),
        ureq::Error::Io(io) if lookup_failed(io) => {
            format!("This computer couldn't look up {host}: it isn't on the internet, or the network's DNS filter blocks the name.")
        }
        ureq::Error::Timeout(_) => format!("{host} didn't answer within {} s: a firewall is probably dropping the connection.", API_TIMEOUT.as_secs()),
        ureq::Error::Io(io) if matches!(io.kind(), TimedOut | WouldBlock) => {
            format!("{host} didn't answer within {} s: a firewall is probably dropping the connection.", API_TIMEOUT.as_secs())
        }
        ureq::Error::ConnectionFailed => format!("The connection to {host} was refused: a firewall is probably blocking it."),
        ureq::Error::Io(io) if matches!(io.kind(), ConnectionRefused | ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof) => {
            format!("The connection to {host} was refused or cut off ({io}): a firewall is probably blocking it.")
        }
        ureq::Error::Rustls(_) | ureq::Error::Tls(_) | ureq::Error::Pem(_) => intercepted(host, e),
        // (rustls's certificate errors come as I/O errors: "invalid peer certificate: UnknownIssuer".)
        ureq::Error::Io(io) if io.to_string().contains("certificate") => intercepted(host, io),
        ureq::Error::ConnectProxyFailed(why) => format!("The proxy wouldn't connect this computer to {host} ({why})."),
        ureq::Error::InvalidProxyUrl => "The proxy set for this computer (HTTPS_PROXY or ALL_PROXY) isn't a web address.".to_string(),
        _ => format!("This computer couldn't reach {host} ({e})."),
    };
    format!("{why} {ASK_IT}")
}

/// A certificate that isn't the site's: something on the network stood in for it.
fn intercepted(host: &str, why: &dyn std::fmt::Display) -> String {
    format!(
        "The secure connection to {host} was intercepted: the certificate this computer was shown isn't {host}'s ({why}). A firewall or web filter that inspects HTTPS does that; it needs to let {host} through untouched."
    )
}

/// A name that couldn't be looked up, as each system's resolver says it.
fn lookup_failed(e: &std::io::Error) -> bool {
    let s = e.to_string().to_ascii_lowercase();
    ["lookup address", "name or service not known", "nodename nor servname", "no such host", "temporary failure in name resolution", "no address associated"]
        .iter()
        .any(|m| s.contains(m))
}

/// A web page's title, or the start of its text: what a block page says it is.
fn page_text(body: &str) -> String {
    let lower = body.to_ascii_lowercase();
    let text = match (lower.find("<title>"), lower.find("</title>")) {
        (Some(a), Some(b)) if b > a + 7 => body[a + 7..b].to_string(),
        _ => {
            let mut out = String::new();
            let mut tag = false;
            for c in body.chars() {
                match c {
                    '<' => tag = true,
                    '>' => {
                        tag = false;
                        out.push(' ');
                    }
                    c if !tag => out.push(c),
                    _ => {}
                }
            }
            out
        }
    };
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut cut: String = text.chars().take(100).collect();
    if text.chars().count() > 100 {
        cut.push('…');
    }
    cut
}

/// Whether this computer can open a connection to the feed's server (municipal networks often let
/// the web through and nothing else). Err: what to tell the user.
pub fn reachable(host: &str, port: u16) -> Result<(), String> {
    let blocked = |why: String| {
        format!(
            "This computer couldn't connect to {host} port {port} ({why}), so the feed won't stream until it can: a firewall is probably blocking it. {ASK_IT}"
        )
    };
    let addrs: Vec<_> = (host, port).to_socket_addrs().map_err(|e| blocked(format!("can't look it up: {e}")))?.collect();
    let mut last = String::from("no address");
    for a in addrs {
        match TcpStream::connect_timeout(&a, CONNECT_TIMEOUT) {
            Ok(_) => return Ok(()),
            Err(e) => last = e.to_string(),
        }
    }
    Err(blocked(last))
}

/// The feed to fill in: the one asked for, or the account's only one. Err: what to say (which
/// feeds there are, to choose from).
pub fn pick(feeds: &[BcfyFeed], id: Option<u32>) -> Result<&BcfyFeed, String> {
    let list = || feeds.iter().map(|f| format!("{} ({})", f.id, f.name.trim())).collect::<Vec<_>>().join(", ");
    match id {
        Some(id) => feeds.iter().find(|f| f.id == id).ok_or_else(|| format!("This account has no feed {id}. Its feeds: {}.", list())),
        None => match feeds {
            [f] => Ok(f),
            _ => Err(format!(
                "This account has {} feeds: {}. Put the one to fill in under Broadcastify feed ID, then press the button again.",
                feeds.len(),
                list()
            )),
        },
    }
}

/// The plugin's feeds (as edited) with `f` filled in: the feed already streaming to its mount
/// gets its server, port and password; otherwise it's added. Also whether it was added.
pub fn fill_in(feeds: &Value, f: &BcfyFeed) -> (Vec<Value>, bool) {
    let mut feeds: Vec<Value> = feeds.as_array().cloned().unwrap_or_default();
    let mount = f.mount();
    let same = |v: &Value| {
        let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").trim().to_string();
        s("server").eq_ignore_ascii_case(&f.host) && format!("/{}", s("mount").trim_start_matches('/')) == mount
    };
    match feeds.iter_mut().find(|v| same(v)) {
        Some(v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("server".into(), json!(f.host));
                o.insert("port".into(), json!(f.port));
                o.insert("mount".into(), json!(mount));
                o.insert("password".into(), json!(f.source_password));
            }
            (feeds, false)
        }
        None => {
            feeds.push(json!({ "name": f.name.trim(), "server": f.host, "port": f.port, "mount": mount, "password": f.source_password }));
            (feeds, true)
        }
    }
}

/// The host a URL names.
fn host_of(url: &str) -> String {
    url.split_once("://").map_or(url, |(_, r)| r).split(['/', ':']).next().unwrap_or("").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use trunk_recorder_plugin::testing::{MockServer, Request};

    const FEEDS: &str = r#"[{"feedId":"38565","feedDescr":"Augusta Fire","mount":"abcd1234","source_password":"s3cret","hostname_ext":"audio9.broadcastify.com","port_ext":"80"},
        {"feedId":40001,"feedDescr":"Augusta Police","mount":"/efgh5678","source_password":"other","hostname_ext":"audio3.broadcastify.com","port_ext":80}]"#;

    fn mock(status: u16, body: &'static str) -> MockServer {
        MockServer::start(move |_: &Request| (status, body.to_string()))
    }

    #[test]
    fn an_accounts_feeds() {
        let server = mock(200, FEEDS);
        let feeds = feeds(server.url(), "me", "pw p&w").unwrap();
        assert_eq!(feeds.len(), 2);
        assert_eq!(
            feeds[0],
            BcfyFeed {
                id: 38565,
                name: "Augusta Fire".into(),
                mount: "abcd1234".into(),
                source_password: "s3cret".into(),
                host: "audio9.broadcastify.com".into(),
                port: 80
            }
        );
        assert_eq!((feeds[0].mount(), feeds[1].mount()), ("/abcd1234".to_string(), "/efgh5678".to_string()));
        // The login goes in the form, not the address.
        let r = &server.requests()[0];
        assert_eq!(r.method, "POST");
        assert!(!r.path.contains("me") && !r.path.contains("pw"), "{}", r.path);
        let body = String::from_utf8_lossy(&r.body).to_string();
        assert!(body.contains("a=get_user_feeds") && body.contains("u=me") && body.contains("p=pw+p%26w"), "{body}");
    }

    #[test]
    fn what_broadcastify_says() {
        let e = |body: &str| answer("api.broadcastify.com", 200, body).unwrap_err();
        assert!(e(r#"{"error":true,"errorMsg":"Invalid Username or Password"}"#).contains("didn't accept that username and password"));
        assert!(e(r#"{"error":true,"errorMsg":"No feeds available for user"}"#).contains("no feeds"));
        assert!(e(r#"{"error":true,"errorMsg":"Something new"}"#).contains("Broadcastify says: Something new"));
        assert!(e("[]").contains("no feeds"));
    }

    /// Something else answering in Broadcastify's place (a block page, a proxy) is said to be
    /// the network, with what to ask for.
    #[test]
    fn a_network_in_the_way() {
        let block = "<html><head><title>Web Page Blocked</title></head><body>Access to this site is blocked by your organization's policy.</body></html>";
        for (status, body, says) in [
            (403, block, "HTTP 403 (\"Web Page Blocked\")"),
            (200, block, "a page that isn't Broadcastify's (\"Web Page Blocked\")"),
            (200, "<p>Access Denied</p>", "(\"Access Denied\")"),
            (502, "", "HTTP 502"),
            (407, "", "A proxy on your network wants a login"),
        ] {
            let m = answer("api.broadcastify.com", status, body).unwrap_err();
            assert!(m.contains(says), "{status}: {m}");
            assert!(m.contains("ask its IT staff to allow outbound HTTPS (port 443) to api.broadcastify.com"), "{m}");
        }
        // Through a real request: a block page from the server in Broadcastify's place.
        let server = mock(403, "<title>Access Denied</title>");
        let m = feeds(server.url(), "me", "pw").unwrap_err();
        assert!(m.contains("answered in place of 127.0.0.1") && m.contains("Access Denied"), "{m}");
    }

    /// Nothing answering, or a refused connection, is a firewall's doing; so is a name that
    /// can't be looked up.
    #[test]
    fn no_way_out() {
        // A port nothing listens on.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let m = feeds(&format!("http://127.0.0.1:{port}/appl/v1/"), "me", "pw").unwrap_err();
        assert!(m.contains("refused") && m.contains("firewall") && m.contains("IT staff"), "{m}");
        let m = feeds("http://no-such-host.invalid/appl/v1/", "me", "pw").unwrap_err();
        assert!(m.contains("couldn't look up no-such-host.invalid") && m.contains("IT staff"), "{m}");
        assert!(!m.contains("pw"), "the password is never said");
    }

    /// Broadcastify's own API, turning down a login that isn't one. Needs the internet:
    /// `cargo test -- --ignored the_real`.
    #[test]
    #[ignore]
    fn the_real_api_turns_down_a_wrong_login() {
        let m = feeds(API, "no-such-user-icecast-endpoint-test", "x").unwrap_err();
        assert!(m.contains("didn't accept that username and password"), "{m}");
    }

    /// A certificate that isn't the site's, as a firewall that inspects HTTPS shows: said to be
    /// intercepted. Needs the internet (badssl.com).
    #[test]
    #[ignore]
    fn an_intercepted_connection() {
        let m = feeds("https://self-signed.badssl.com/", "me", "pw").unwrap_err();
        assert!(m.contains("The secure connection to self-signed.badssl.com was intercepted") && m.contains("IT staff"), "{m}");
    }

    #[test]
    fn whether_the_feeds_server_takes_a_connection() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        assert_eq!(reachable("127.0.0.1", l.local_addr().unwrap().port()), Ok(()));
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let m = reachable("127.0.0.1", port).unwrap_err();
        assert!(m.contains(&format!("couldn't connect to 127.0.0.1 port {port}")) && m.contains("firewall"), "{m}");
    }

    #[test]
    fn which_feed() {
        let feeds = answer("x", 200, FEEDS).unwrap();
        assert_eq!(pick(&feeds, Some(40001)).unwrap().id, 40001);
        let m = pick(&feeds, None).unwrap_err();
        assert!(m.contains("2 feeds: 38565 (Augusta Fire), 40001 (Augusta Police)") && m.contains("Broadcastify feed ID"), "{m}");
        assert!(pick(&feeds, Some(1)).unwrap_err().contains("no feed 1"));
        assert_eq!(pick(&feeds[..1], None).unwrap().id, 38565, "the only one");
    }

    #[test]
    fn filled_in() {
        let f = &answer("x", 200, FEEDS).unwrap()[0];
        // Added, after the feeds as edited.
        let (feeds, added) = fill_in(&json!([{ "name": "Other", "server": "x.example", "mount": "/x", "password": "p", "volume": 2 }]), f);
        assert!(added);
        assert_eq!(feeds[0]["volume"], 2, "the others as they were");
        assert_eq!(feeds[1], json!({ "name": "Augusta Fire", "server": "audio9.broadcastify.com", "port": 80, "mount": "/abcd1234", "password": "s3cret" }));
        // The feed already on its mount gets the server's details, and keeps the rest.
        let had = json!([{ "name": "Mine", "server": "AUDIO9.broadcastify.com", "port": 8000, "mount": "abcd1234", "password": "old", "updateTitle": true }]);
        let (feeds, added) = fill_in(&had, f);
        assert!(!added);
        assert_eq!(feeds.len(), 1);
        assert_eq!(
            (&feeds[0]["name"], &feeds[0]["port"], &feeds[0]["password"], &feeds[0]["updateTitle"]),
            (&json!("Mine"), &json!(80), &json!("s3cret"), &json!(true))
        );
        assert_eq!(fill_in(&Value::Null, f).0.len(), 1, "no feeds yet");
    }
}
