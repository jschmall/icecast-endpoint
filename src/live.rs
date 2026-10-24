//! Tests against a real Icecast server. They're ignored by `cargo test`;
//! start one with `scripts/icecast.sh up`, then run `cargo test -- --ignored`.
//! ICECAST (default 127.0.0.1:8000) and ICECAST_PASSWORD (hackme) say where it is.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use trunk_recorder_plugin::{AudioChunk, CallInfo, HostMessage, Level, State, base64, testing};

use crate::IcecastEndpoint;
use crate::encode::frames;
use crate::feed::{Feed, Health, Msg, Pan, Settings};
use crate::icecast::{Protocol, StreamInfo, Target};

fn server() -> (String, u16) {
    let s = std::env::var("ICECAST").unwrap_or_else(|_| "127.0.0.1:8000".into());
    let (h, p) = s.rsplit_once(':').expect("ICECAST is host:port");
    (h.to_string(), p.parse().expect("a port"))
}

fn password() -> String {
    std::env::var("ICECAST_PASSWORD").unwrap_or_else(|_| "hackme".into())
}

fn target(mount: &str, protocol: Protocol) -> Target {
    let (host, port) = server();
    Target {
        host,
        port,
        mount: mount.into(),
        user: "source".into(),
        password: password(),
        protocol,
        info: StreamInfo { name: "Live test".into(), genre: "Scanner".into(), rate: 22050, kbps: 16, ..Default::default() },
    }
}

fn start(t: Target, update_title: bool) -> (Feed, testing::Captured) {
    let (host, captured) = testing::capture();
    let s = Settings { name: format!("Live test ({t})"), target: t, max_delay: Duration::from_secs(60), update_title };
    (Feed::start(s, Health::new(host)).unwrap(), captured)
}

/// A GET to the server: (status, head, body), reading the body for up to `for_`.
fn get(path: &str, auth: Option<&str>, for_: Duration) -> (u16, String, Vec<u8>) {
    let (host, port) = server();
    let mut s = TcpStream::connect((host.as_str(), port)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    let auth = auth.map(|a| format!("Authorization: Basic {}\r\n", base64::encode(a.as_bytes()))).unwrap_or_default();
    write!(s, "GET {path} HTTP/1.0\r\nHost: {host}:{port}\r\n{auth}\r\n").unwrap();
    let mut all = Vec::new();
    let until = Instant::now() + for_;
    let mut buf = [0u8; 8192];
    while Instant::now() < until {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => all.extend_from_slice(&buf[..n]),
            Err(_) => {}
        }
    }
    let split = all.windows(4).position(|w| w == b"\r\n\r\n").expect("a response head");
    let head = String::from_utf8_lossy(&all[..split]).to_string();
    let code = head.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    (code, head, all[split + 4..].to_vec())
}

/// The server's entry for `mount`, once it has one (or after 5 s, None).
fn source(mount: &str) -> Option<Value> {
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        let (_, _, body) = get("/status-json.xsl", None, Duration::from_millis(500));
        let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let sources = match &v["icestats"]["source"] {
            Value::Array(a) => a.clone(),
            Value::Null => vec![],
            one => vec![one.clone()],
        };
        if let Some(s) = sources.into_iter().find(|s| s["listenurl"].as_str().is_some_and(|u| u.ends_with(mount))) {
            return Some(s);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn listen_and_check(mount: &str) {
    let (code, head, body) = get(mount, None, Duration::from_secs(3));
    assert_eq!(code, 200, "{head}");
    assert!(head.to_ascii_lowercase().contains("content-type: audio/mpeg"), "{head}");
    let f = frames(&body).unwrap();
    assert!(f.len() > 20, "{} frames", f.len());
    assert!(f.iter().all(|f| f.rate == 22050 && f.kbps == 16 && f.mono), "{:?}", f[0]);
}

fn connected_with(captured: &testing::Captured) -> String {
    let until = soon();
    loop {
        let logs = captured.output().logs();
        if let Some((_, m)) = logs.iter().find(|(l, m)| *l == Level::Info && m.starts_with("streaming to")) {
            return m.clone();
        }
        assert!(Instant::now() < until, "never connected: {logs:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
#[ignore]
fn streams_with_put() {
    let (feed, captured) = start(target("/live-put", Protocol::Put), false);
    assert!(connected_with(&captured).ends_with("(PUT)"));
    let s = source("/live-put").expect("the mount is up");
    assert_eq!(s["server_type"], "audio/mpeg");
    assert_eq!(s["server_name"], "Live test");
    assert_eq!(s["genre"], "Scanner");
    assert_eq!(s["audio_info"], "channels=1;samplerate=22050;bitrate=16");
    listen_and_check("/live-put");
    feed.finish(soon());
}

#[test]
#[ignore]
fn streams_with_source() {
    let (feed, captured) = start(target("/live-source", Protocol::Source), false);
    assert!(connected_with(&captured).ends_with("(SOURCE)"));
    assert!(source("/live-source").is_some());
    listen_and_check("/live-source");
    feed.finish(soon());
}

#[test]
#[ignore]
fn streams_stereo() {
    let mut t = target("/live-stereo", Protocol::Auto);
    (t.info.stereo, t.info.kbps) = (true, 32);
    let (feed, _) = start(t, false);
    feed.send(Msg::Start { id: 1, title: String::new(), pan: Pan::Left });
    feed.send(Msg::Start { id: 2, title: String::new(), pan: Pan::Right });
    for _ in 0..50 {
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: vec![1000; 160] });
        feed.send(Msg::Audio { id: 2, rate: 8000, samples: vec![-1000; 160] });
    }
    let s = source("/live-stereo").expect("the mount is up");
    assert_eq!(s["audio_info"], "channels=2;samplerate=22050;bitrate=32");
    let (code, head, body) = get("/live-stereo", None, Duration::from_secs(3));
    assert_eq!(code, 200, "{head}");
    let f = frames(&body).unwrap();
    assert!(f.len() > 20, "{} frames", f.len());
    assert!(f.iter().all(|f| f.rate == 22050 && f.kbps == 32 && !f.mono && !f.joint), "{:?}", f[0]);
    feed.finish(soon());
}

#[test]
#[ignore]
fn automatic_uses_put() {
    let (feed, captured) = start(target("/live-auto", Protocol::Auto), false);
    assert!(connected_with(&captured).ends_with("(PUT)"));
    feed.finish(soon());
}

#[test]
#[ignore]
fn the_title_follows_the_call() {
    let (feed, _) = start(target("/live-title", Protocol::Auto), true);
    assert!(source("/live-title").is_some());
    feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into(), pan: Pan::Center });
    for _ in 0..50 {
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: vec![1000; 160] });
    }
    let until = soon();
    let mut title = Value::Null;
    while Instant::now() < until {
        title = source("/live-title").unwrap()["title"].clone();
        if title == "Fire Dispatch (101)" {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(title, "Fire Dispatch (101)");
    feed.finish(soon());
}

#[test]
#[ignore]
fn a_wrong_password() {
    let mut t = target("/live-bad", Protocol::Auto);
    t.password = "not-it".into();
    let (feed, captured) = start(t, false);
    let until = soon();
    while captured.output().status().is_none() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(50));
    }
    let (state, msg) = captured.output().status().unwrap();
    assert_eq!(state, State::Warning);
    assert!(msg.contains("wrong username or password"), "{msg}");
    assert!(!format!("{:?}", captured.output().messages).contains("not-it"));
    feed.finish(soon());
}

#[test]
#[ignore]
fn comes_back_when_the_server_drops_it() {
    let (feed, captured) = start(target("/live-kill", Protocol::Auto), false);
    assert!(source("/live-kill").is_some());
    let (code, head, _) = get("/admin/killsource?mount=/live-kill", Some(&format!("admin:{}", password())), Duration::from_millis(500));
    assert_eq!(code, 200, "{head}");
    let until = Instant::now() + Duration::from_secs(10);
    while Instant::now() < until {
        let logs = captured.output().logs();
        if logs.iter().filter(|(l, m)| *l == Level::Info && m.starts_with("streaming to")).count() >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let logs = captured.output().logs();
    assert!(logs.iter().any(|(l, m)| *l == Level::Warn && m.contains("lost the connection")), "{logs:?}");
    assert_eq!(logs.iter().filter(|(l, m)| *l == Level::Info && m.starts_with("streaming to")).count(), 2, "{logs:?}");
    assert!(source("/live-kill").is_some());
    feed.finish(soon());
}

/// The whole plugin, as the recorder runs it: calls in, a stream out.
#[test]
#[ignore]
fn the_plugin_streams_calls() {
    let (host, port) = server();
    let feeds = json!([{ "name": "Live", "server": host, "port": port, "mount": "/live-plugin", "password": password() }]);
    let call = CallInfo { id: 1, short_name: "sys1".into(), talkgroup: 101, talkgroup_tag: "Fire Dispatch".into(), recording: true, ..Default::default() };
    let mut m =
        vec![HostMessage::Hello(testing::hello(&testing::temp_dir("icecast-endpoint"), json!({ "feeds": feeds }))), HostMessage::CallStart(call.clone())];
    m.extend((0..100).map(|_| HostMessage::Audio(AudioChunk { short_name: "sys1".into(), ..AudioChunk::new(1, 0, 101, 8000, &[1000; 160]) })));
    m.push(HostMessage::CallEnd(call));
    let out = testing::run::<IcecastEndpoint>(m);
    assert!(out.ready(), "{:?}", out.messages);
    assert_eq!(out.status().unwrap().0, State::Ok, "{:?}", out.messages);
    let metrics = out.metrics();
    let last = metrics.last().unwrap();
    assert_eq!(last.extra["callsPlayed"], 1);
    // 2 s of call: about 4 KB at 16 kbps.
    assert!(last.bytes_sent.unwrap() > 3500, "{:?}", last.bytes_sent);
}
