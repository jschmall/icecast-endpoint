//! One feed: a thread of its own that keeps an MP3 stream going to an
//! Icecast mount, in real time. Calls' audio is played as the scheduler
//! says, and silence when there's none, so the server never times the
//! source out. A slow or missing server never holds up the plugin.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use trunk_recorder_plugin::{Endpoint, EndpointState, Host, Metrics, State};

use crate::encode::Mp3;
use crate::icecast::{self, Method, Target};
use crate::schedule::Scheduler;

/// What the plugin tells a feed.
pub enum Msg {
    Start { id: u32, title: String },
    Audio { id: u32, rate: u32, samples: Vec<i16> },
    End { id: u32 },
}

pub struct Settings {
    pub target: Target,
    /// How it's known in the log: its name and where it goes, never its password.
    pub name: String,
    pub max_delay: Duration,
    pub update_title: bool,
}

/// Messages waiting for a feed (at 50 chunks a second, over a minute of a call).
const QUEUE: usize = 4096;
/// How often the feed's thread wakes to send what's due.
const TICK: Duration = Duration::from_millis(20);
/// Waits between tries to reconnect: doubling from the first to the last.
const BACKOFF: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(60));
/// A connection that has lasted this long resets the backoff.
const STEADY: Duration = Duration::from_secs(60);
/// Metrics go to the recorder's dashboard at most this often.
const METRICS_EVERY: Duration = Duration::from_secs(10);

pub struct Feed {
    tx: Option<SyncSender<Msg>>,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
    name: String,
    health: Health,
}

impl Feed {
    /// Start streaming. Errs if the stream's format can't be encoded.
    pub fn start(s: Settings, health: Health) -> Result<Feed, String> {
        let info = &s.target.info;
        Mp3::new(info.rate, info.kbps)?;
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        let stop = Arc::new(AtomicBool::new(false));
        let name = s.name.clone();
        health.add(&name);
        let (h, st) = (health.clone(), stop.clone());
        let thread = std::thread::Builder::new()
            .name(format!("feed {}", s.target))
            .spawn(move || run(s, rx, &h, &st))
            .map_err(|e| format!("can't start a thread: {e}"))?;
        Ok(Feed { tx: Some(tx), stop, thread, name, health })
    }

    /// Hand the feed a message (dropped, and counted, if it's far behind).
    pub fn send(&self, m: Msg) {
        if let Some(tx) = &self.tx
            && tx.try_send(m).is_err()
        {
            self.health.update(&self.name, |f| f.chunks_dropped += 1);
        }
    }

    /// Take no more messages: play what's waiting, then close the stream.
    pub fn close(&mut self) {
        drop(self.tx.take());
    }

    /// Close, and wait (until `deadline`) for the stream to finish.
    pub fn finish(mut self, deadline: Instant) {
        self.close();
        while !self.thread.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if self.thread.is_finished() {
            let _ = self.thread.join();
        } else {
            self.stop.store(true, Ordering::Relaxed);
        }
    }
}

fn run(s: Settings, rx: Receiver<Msg>, health: &Health, stop: &AtomicBool) {
    let Settings { target, name, max_delay, update_title } = s;
    let (rate, kbps) = (target.info.rate, target.info.kbps);
    let Ok(mut enc) = Mp3::new(rate, kbps) else { return };
    let mut sched = Scheduler::new(rate, max_delay);
    let mut conn: Option<(TcpStream, Instant)> = None;
    let mut use_source = false;
    let mut backoff = BACKOFF.0;
    let mut retry_at = Instant::now();
    // The stream's clock: samples are due at `rate` from `since`.
    let (mut since, mut sent) = (Instant::now(), 0u64);
    let mut title: Option<String> = None;
    let title_busy = Arc::new(AtomicBool::new(false));
    let title_warned = Arc::new(AtomicBool::new(false));
    let mut pcm = Vec::new();
    let mut closing = false;
    while !stop.load(Ordering::Relaxed) {
        let tick = Instant::now();
        loop {
            match rx.try_recv() {
                Ok(Msg::Start { id, title }) => sched.start(id, title, tick),
                Ok(Msg::Audio { id, rate, samples }) => sched.audio(id, rate, &samples, tick),
                Ok(Msg::End { id }) => sched.end(id),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    closing = true;
                    break;
                }
            }
        }
        if conn.is_none() && (closing || tick >= retry_at) {
            match icecast::open(&target, &mut use_source) {
                Ok((c, method)) => {
                    let how = if method == Method::Put { "PUT" } else { "SOURCE" };
                    health.up(&name, format!("streaming to {name} ({how})"));
                    conn = Some((c, Instant::now()));
                    // A new stream: a fresh encoder, and its own clock.
                    if let Ok(e) = Mp3::new(rate, kbps) {
                        enc = e;
                    }
                    (since, sent, title) = (Instant::now(), 0, None);
                }
                Err(e) => {
                    health.down(&name, e);
                    retry_at = Instant::now() + backoff;
                    backoff = (backoff * 2).min(BACKOFF.1);
                }
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        pcm.clear();
        if closing {
            // Everything waiting goes now, as fast as the server takes it.
            for _ in 0..1000 {
                let n = sched.backlog();
                if n == 0 {
                    break;
                }
                sched.pull(n, tick, &mut pcm);
            }
            // Let a call that has ended count as played.
            sched.pull(1, tick, &mut pcm);
        } else {
            let due = (since.elapsed().as_secs_f64() * rate as f64) as u64;
            let mut owed = due.saturating_sub(sent);
            // After a stall (a slow connect), don't send a burst: carry on from now.
            if owed > rate as u64 {
                sent += owed - rate as u64;
                owed = rate as u64;
            }
            sched.pull(owed as usize, tick, &mut pcm);
            sent += owed;
        }
        let mut mp3 = match enc.encode(&pcm) {
            Ok(b) => b.to_vec(),
            Err(e) => {
                health.down(&name, format!("{name}: {e}"));
                Vec::new()
            }
        };
        if closing {
            mp3.extend(enc.flush());
        }
        if let Some((c, at)) = conn.as_mut() {
            match c.write_all(&mp3) {
                Ok(()) => {
                    health.update(&name, |f| f.bytes += mp3.len() as u64);
                    if at.elapsed() >= STEADY {
                        backoff = BACKOFF.0;
                    }
                }
                Err(e) => {
                    health.down(&name, format!("{name}: lost the connection ({e})"));
                    conn = None;
                    retry_at = Instant::now() + backoff;
                    backoff = (backoff * 2).min(BACKOFF.1);
                }
            }
        }
        if update_title && conn.is_some() {
            let playing = sched.playing().map(String::from);
            if playing.is_some() && playing != title && !title_busy.swap(true, Ordering::Relaxed) {
                title = playing.clone();
                let (t, busy, warned, h, n) = (target.clone(), title_busy.clone(), title_warned.clone(), health.clone(), name.clone());
                std::thread::spawn(move || {
                    if let Err(e) = icecast::update_title(&t, playing.as_deref().unwrap_or_default())
                        && !warned.swap(true, Ordering::Relaxed)
                    {
                        h.host.warn(format!("{e} (the stream goes on; titles may not be supported by {n})"));
                    }
                    busy.store(false, Ordering::Relaxed);
                });
            }
        }
        let (played, dropped) = sched.counts();
        let backlog = sched.backlog() as f64 / rate as f64;
        health.update(&name, |f| (f.played, f.calls_dropped, f.backlog) = (played, dropped, backlog));
        if closing {
            break;
        }
        std::thread::sleep(TICK.saturating_sub(tick.elapsed()));
    }
    if let Some((c, _)) = conn {
        let _ = c.shutdown(Shutdown::Both);
    }
    health.report();
}

/// What one feed has done.
#[derive(Clone, Debug, Default)]
struct FeedState {
    /// Streaming (Some(true)), can't (Some(false)), or not tried yet.
    up: Option<bool>,
    problem: Option<String>,
    bytes: u64,
    played: u64,
    calls_dropped: u64,
    chunks_dropped: u64,
    /// Seconds of audio waiting.
    backlog: f64,
}

/// The plugin's status (the feeds that aren't streaming, if any) and, for
/// the dashboard, each feed's figures.
#[derive(Clone)]
pub struct Health {
    host: Host,
    feeds: Arc<Mutex<BTreeMap<String, FeedState>>>,
    reported: Arc<Mutex<Option<Instant>>>,
}

impl Health {
    pub fn new(host: Host) -> Health {
        Health { host, feeds: Default::default(), reported: Default::default() }
    }

    fn add(&self, name: &str) {
        self.feeds.lock().unwrap().entry(name.to_string()).or_default();
    }

    /// A feed is streaming.
    fn up(&self, name: &str, message: String) {
        self.host.info(message);
        self.set(name, None);
    }

    /// A feed can't stream, and why.
    fn down(&self, name: &str, why: String) {
        self.set(name, Some(why));
    }

    fn set(&self, name: &str, problem: Option<String>) {
        let mut feeds = self.feeds.lock().unwrap();
        let f = feeds.entry(name.to_string()).or_default();
        let up = Some(problem.is_none());
        if f.up == up && f.problem == problem {
            return;
        }
        if let Some(why) = &problem
            && f.problem.as_ref() != Some(why)
        {
            self.host.warn(why.clone());
        }
        (f.up, f.problem) = (up, problem);
        let problems: Vec<String> = feeds.values().filter_map(|f| f.problem.clone()).collect();
        if problems.is_empty() {
            let names: Vec<&str> = feeds.iter().filter(|(_, f)| f.up == Some(true)).map(|(n, _)| n.as_str()).collect();
            self.host.status(State::Ok, format!("streaming to {}", names.join(", ")));
        } else {
            self.host.status(State::Warning, problems.join("; "));
        }
    }

    fn update(&self, name: &str, f: impl FnOnce(&mut FeedState)) {
        f(self.feeds.lock().unwrap().entry(name.to_string()).or_default());
        let mut r = self.reported.lock().unwrap();
        if r.is_none_or(|t| t.elapsed() >= METRICS_EVERY) {
            *r = Some(Instant::now());
            drop(r);
            self.host.metrics(&self.metrics());
        }
    }

    /// Send the dashboard's figures now.
    pub fn report(&self) {
        *self.reported.lock().unwrap() = Some(Instant::now());
        self.host.metrics(&self.metrics());
    }

    /// The dashboard's figures.
    pub fn metrics(&self) -> Metrics {
        let feeds = self.feeds.lock().unwrap().clone();
        let total = |f: fn(&FeedState) -> u64| feeds.values().map(f).sum::<u64>();
        let endpoints = feeds
            .iter()
            .map(|(name, f)| {
                let state = match f.up {
                    Some(true) => EndpointState::Up,
                    Some(false) => EndpointState::Down,
                    None => EndpointState::Unknown,
                };
                Endpoint { name: name.clone(), state, last_error: f.problem.clone().unwrap_or_default(), ..Default::default() }
            })
            .collect();
        let mut extra = BTreeMap::new();
        extra.insert("callsPlayed".to_string(), total(|f| f.played).into());
        extra.insert("callsDropped".to_string(), total(|f| f.calls_dropped).into());
        extra.insert("chunksDropped".to_string(), total(|f| f.chunks_dropped).into());
        let backlog = feeds.values().map(|f| f.backlog).fold(0.0, f64::max);
        extra.insert("backlogSeconds".to_string(), ((backlog * 10.0).round() / 10.0).into());
        Metrics { bytes_sent: Some(total(|f| f.bytes)), endpoints, extra, ..Default::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::frames;
    use crate::icecast::{Protocol, StreamInfo, fake};
    use trunk_recorder_plugin::{Level, testing};

    fn settings(port: u16) -> Settings {
        let target = Target {
            host: "127.0.0.1".into(),
            port,
            mount: "/feed".into(),
            user: "source".into(),
            password: "hackme".into(),
            protocol: Protocol::Auto,
            info: StreamInfo { name: "County".into(), rate: 22050, kbps: 16, ..Default::default() },
        };
        Settings { name: format!("County (127.0.0.1:{port}/feed)"), target, max_delay: Duration::from_secs(60), update_title: false }
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn tone(n: usize) -> Vec<i16> {
        (0..n).map(|i| ((i as f64 * 0.7).sin() * 8000.0) as i16).collect()
    }

    #[test]
    fn silence_keeps_the_stream_going_in_real_time() {
        let server = fake::Server::ok();
        let (host, _) = testing::capture();
        let feed = Feed::start(settings(server.port), Health::new(host)).unwrap();
        server.wait(|c| c.first().is_some_and(|c| !c.body.is_empty()));
        std::thread::sleep(Duration::from_millis(1500));
        let first = server.conns()[0].body.len();
        feed.finish(soon());
        let body = &server.conns()[0].body;
        let f = frames(body).unwrap();
        assert!(f.iter().all(|f| f.rate == 22050 && f.kbps == 16 && f.mono));
        // 16 kbps is 2000 bytes a second: about 1.5 s of it, not a burst.
        assert!((2000..4500).contains(&first), "{first} bytes");
    }

    #[test]
    fn plays_a_call_and_finishes_it_at_shutdown() {
        let server = fake::Server::ok();
        let (host, captured) = testing::capture();
        let health = Health::new(host);
        let feed = Feed::start(settings(server.port), health.clone()).unwrap();
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into() });
        // 2 s of the call, in the recorder's 20 ms chunks.
        for c in tone(16000).chunks(160) {
            feed.send(Msg::Audio { id: 1, rate: 8000, samples: c.to_vec() });
        }
        feed.send(Msg::End { id: 1 });
        feed.finish(soon());
        let c = &server.wait(|c| c.first().is_some_and(|c| c.closed))[0];
        assert!(c.closed);
        // All 2 s of it went (576 samples a frame), though it was stopped at once.
        let seconds = frames(&c.body).unwrap().len() as f64 * 576.0 / 22050.0;
        assert!(seconds >= 1.95, "{seconds} s");
        let m = health.metrics();
        assert_eq!(m.extra["callsPlayed"], 1);
        assert_eq!(m.bytes_sent.unwrap(), c.body.len() as u64);
        assert_eq!(m.endpoints[0].state, EndpointState::Up);
        let out = captured.output();
        assert!(out.logs().iter().any(|(l, m)| *l == Level::Info && m.contains("streaming to County")), "{:?}", out.logs());
    }

    #[test]
    fn reconnects_when_the_server_hangs_up() {
        let server = fake::Server::ok();
        let (host, captured) = testing::capture();
        let feed = Feed::start(settings(server.port), Health::new(host)).unwrap();
        server.wait(|c| c.first().is_some_and(|c| !c.body.is_empty()));
        server.kick();
        let c = server.wait(|c| c.len() == 2 && !c[1].body.is_empty());
        assert_eq!(c.len(), 2, "it didn't come back");
        feed.finish(soon());
        let out = captured.output();
        assert!(out.logs().iter().any(|(l, m)| *l == Level::Warn && m.contains("lost the connection")), "{:?}", out.logs());
        assert_eq!(out.status().unwrap().0, State::Ok);
    }

    #[test]
    fn a_server_thats_down_is_a_warning() {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let (host, captured) = testing::capture();
        let health = Health::new(host);
        let feed = Feed::start(settings(port), health.clone()).unwrap();
        let until = soon();
        while captured.output().status().is_none() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(10));
        }
        let (state, msg) = captured.output().status().unwrap();
        assert_eq!(state, State::Warning);
        assert!(msg.contains("can't connect to 127.0.0.1"), "{msg}");
        assert_eq!(health.metrics().endpoints[0].state, EndpointState::Down);
        feed.finish(soon());
        let all = format!("{:?}", captured.output().messages);
        assert!(!all.contains("hackme"));
    }

    #[test]
    fn titles_follow_the_call_playing() {
        let server = fake::Server::ok();
        let (host, _) = testing::capture();
        let feed = Feed::start(Settings { update_title: true, ..settings(server.port) }, Health::new(host)).unwrap();
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into() });
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: tone(800) });
        let c = server.wait(|c| c.iter().any(|c| c.head.starts_with("GET /admin/metadata")));
        feed.finish(soon());
        let title = c.iter().find(|c| c.head.starts_with("GET")).expect("a title update");
        assert!(title.head.contains("song=Fire%20Dispatch%20%28101%29"), "{}", title.head);
    }

    #[test]
    fn no_titles_unless_asked() {
        let server = fake::Server::ok();
        let (host, _) = testing::capture();
        let feed = Feed::start(settings(server.port), Health::new(host)).unwrap();
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into() });
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: tone(800) });
        std::thread::sleep(Duration::from_millis(300));
        feed.finish(soon());
        assert!(server.conns().iter().all(|c| !c.head.starts_with("GET")));
    }

    #[test]
    fn a_server_that_never_answers_doesnt_hold_up_shutdown() {
        let server = fake::Server::start(|_| {
            std::thread::sleep(Duration::from_secs(30));
            String::new()
        });
        let (host, _) = testing::capture();
        let feed = Feed::start(settings(server.port), Health::new(host)).unwrap();
        server.wait(|c| !c.is_empty());
        let t = Instant::now();
        feed.finish(Instant::now() + Duration::from_millis(500));
        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    }

    #[test]
    fn a_format_mp3_doesnt_have_wont_start() {
        let (host, _) = testing::capture();
        let mut s = settings(9);
        s.target.info.kbps = 17;
        assert!(Feed::start(s, Health::new(host)).is_err());
    }
}
