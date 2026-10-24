//! One feed: a thread of its own that keeps an MP3 stream going to an
//! Icecast mount, in real time. Calls' audio is played as the scheduler
//! says, and silence when there's none, so the server never times the
//! source out. A slow or missing server never holds up the plugin.
//!
//! A stereo feed has three schedulers: calls on the left, in the center and
//! on the right each play one at a time, and the three play at once, the
//! center in both ears.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use trunk_recorder_plugin::{Endpoint, EndpointState, Host, Metrics, Outcome, State};

use crate::encode::Mp3;
use crate::icecast::{self, Method, Target};
use crate::schedule::Scheduler;

/// Where a call is heard on a stereo feed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Pan {
    Left,
    Center,
    Right,
}

/// What the plugin tells a feed.
pub enum Msg {
    Start { id: u32, title: String, pan: Pan },
    Audio { id: u32, rate: u32, samples: Vec<i16> },
    End { id: u32 },
}

pub struct Settings {
    pub target: Target,
    /// How it's known in the log: its name and where it goes, never its password.
    pub name: String,
    pub max_delay: Duration,
    pub update_title: bool,
    /// How much louder (or quieter) than recorded: 1 is as recorded.
    pub volume: f32,
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
/// Calls waiting for a result at most; beyond this, ones older than `RESULT_AFTER` are forgotten.
const MAX_PENDING: usize = 1000;
const RESULT_AFTER: Duration = Duration::from_secs(900);

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
        Mp3::new(info.rate, info.kbps, info.stereo)?;
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
    let Settings { target, name, max_delay, update_title, volume } = s;
    let (rate, kbps, stereo) = (target.info.rate, target.info.kbps, target.info.stereo);
    let Ok(mut enc) = Mp3::new(rate, kbps, stereo) else { return };
    let mut mix = Mix::new(rate, max_delay, stereo, volume);
    let mut conn: Option<(TcpStream, Instant)> = None;
    let mut use_source = false;
    let mut backoff = BACKOFF.0;
    let mut retry_at = Instant::now();
    // The stream's clock: samples are due at `rate` from `since`.
    let (mut since, mut sent) = (Instant::now(), 0u64);
    // The title on the server, and the one it should have: that of the call that began playing last.
    let (mut title, mut want): (Option<String>, Option<String>) = (None, None);
    let mut seen: Vec<Option<String>> = vec![None; mix.lanes.len()];
    let title_busy = Arc::new(AtomicBool::new(false));
    let title_warned = Arc::new(AtomicBool::new(false));
    let (mut left, mut right) = (Vec::new(), Vec::new());
    let mut closing = false;
    // Calls that played, in part, while there was no stream to hear them on.
    let mut unheard = HashSet::new();
    while !stop.load(Ordering::Relaxed) {
        let tick = Instant::now();
        loop {
            match rx.try_recv() {
                Ok(Msg::Start { id, title, pan }) => mix.lane(pan).start(id, title, tick),
                Ok(Msg::Audio { id, rate, samples }) => mix.audio(id, rate, &samples, tick),
                Ok(Msg::End { id }) => mix.lanes.iter_mut().for_each(|l| l.end(id)),
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
                    if let Ok(e) = Mp3::new(rate, kbps, stereo) {
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
        left.clear();
        right.clear();
        if closing {
            mix.drain(tick, &mut left, &mut right);
        } else {
            let due = (since.elapsed().as_secs_f64() * rate as f64) as u64;
            let mut owed = due.saturating_sub(sent);
            // After a stall (a slow connect), don't send a burst: carry on from now.
            if owed > rate as u64 {
                sent += owed - rate as u64;
                owed = rate as u64;
            }
            mix.pull(owed as usize, tick, &mut left, &mut right);
            sent += owed;
        }
        let encoded = if stereo { enc.encode_stereo(&left, &right) } else { enc.encode(&left) };
        let mut mp3 = match encoded {
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
        if conn.is_none() {
            unheard.extend(mix.lanes.iter().filter_map(|l| l.playing_id()));
        }
        for lane in &mut mix.lanes {
            for (id, played) in lane.take_done() {
                let lost = unheard.remove(&id) || conn.is_none();
                let failure = if !played {
                    Some(format!("{name}: dropped, with more than {} s of calls waiting", max_delay.as_secs()))
                } else if lost {
                    Some(format!("{name}: not streamed, no connection to the server"))
                } else {
                    None
                };
                health.done(id, failure);
            }
        }
        if update_title && conn.is_some() {
            for (lane, seen) in mix.lanes.iter().zip(seen.iter_mut()) {
                let playing = lane.playing().map(String::from);
                if playing != *seen {
                    *seen = playing.clone();
                    want = playing.or(want.take());
                }
            }
            if want.is_some() && want != title && !title_busy.swap(true, Ordering::Relaxed) {
                title = want.clone();
                let playing = want.clone();
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
        let (played, dropped) = mix.lanes.iter().map(|l| l.counts()).fold((0, 0), |a, c| (a.0 + c.0, a.1 + c.1));
        let backlog = mix.lanes.iter().map(|l| l.backlog()).max().unwrap_or(0) as f64 / rate as f64;
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

/// What a feed plays: one scheduler for mono, or one each for the left,
/// the center and the right.
struct Mix {
    lanes: Vec<Scheduler>,
    /// Each lane's audio, before it's mixed.
    bufs: [Vec<i16>; 3],
    volume: f32,
}

impl Mix {
    fn new(rate: u32, max_delay: Duration, stereo: bool, volume: f32) -> Mix {
        let lanes = (0..if stereo { 3 } else { 1 }).map(|_| Scheduler::new(rate, max_delay)).collect();
        Mix { lanes, bufs: Default::default(), volume }
    }

    fn lane(&mut self, pan: Pan) -> &mut Scheduler {
        let i = match (self.lanes.len(), pan) {
            (1, _) | (_, Pan::Center) => (self.lanes.len()) / 2,
            (_, Pan::Left) => 0,
            (_, Pan::Right) => 2,
        };
        &mut self.lanes[i]
    }

    /// Audio goes to the lane its call is in (the center, for a call it wasn't told of).
    fn audio(&mut self, id: u32, rate: u32, samples: &[i16], now: Instant) {
        let lane = match self.lanes.iter().position(|l| l.knows(id)) {
            Some(i) => &mut self.lanes[i],
            None => self.lane(Pan::Center),
        };
        lane.audio(id, rate, samples, now);
    }

    /// The next `n` samples (`right` is left alone in mono).
    fn pull(&mut self, n: usize, now: Instant, left: &mut Vec<i16>, right: &mut Vec<i16>) {
        for (lane, buf) in self.lanes.iter_mut().zip(self.bufs.iter_mut()) {
            buf.clear();
            lane.pull(n, now, buf);
        }
        self.mix(left, right);
    }

    /// Everything waiting, as fast as it can go.
    fn drain(&mut self, now: Instant, left: &mut Vec<i16>, right: &mut Vec<i16>) {
        for (lane, buf) in self.lanes.iter_mut().zip(self.bufs.iter_mut()) {
            buf.clear();
            lane.end_all();
            for _ in 0..1000 {
                let n = lane.backlog();
                if n == 0 {
                    break;
                }
                lane.pull(n, now, buf);
            }
            // Let a call that has ended count as played.
            lane.pull(1, now, buf);
        }
        let n = self.bufs.iter().map(Vec::len).max().unwrap_or(0);
        self.bufs.iter_mut().for_each(|b| b.resize(n, 0));
        self.mix(left, right);
    }

    /// At the volume, clipped: mono to `left`; in stereo, the left and the
    /// center to the left, the right and the center to the right.
    fn mix(&self, left: &mut Vec<i16>, right: &mut Vec<i16>) {
        let v = self.volume;
        let out = move |a: i16, b: i16| ((a as f32 + b as f32) * v).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        if self.lanes.len() == 1 {
            left.extend(self.bufs[0].iter().map(|&a| out(a, 0)));
            return;
        }
        let [l, c, r] = &self.bufs;
        left.extend(l.iter().zip(c).map(|(&a, &b)| out(a, b)));
        right.extend(r.iter().zip(c).map(|(&a, &b)| out(a, b)));
    }
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

/// A recorded call on its way to a result: its path once its files are
/// written, and what its feeds made of it.
struct Pending {
    path: Option<String>,
    /// Feeds still to finish it.
    feeds: usize,
    failures: Vec<String>,
    since: Instant,
}

/// The plugin's status (the feeds that aren't streaming, if any), each
/// feed's figures for the dashboard, and what became of each call.
#[derive(Clone)]
pub struct Health {
    host: Host,
    feeds: Arc<Mutex<BTreeMap<String, FeedState>>>,
    reported: Arc<Mutex<Option<Instant>>>,
    pending: Arc<Mutex<HashMap<u32, Pending>>>,
}

impl Health {
    pub fn new(host: Host) -> Health {
        Health { host, feeds: Default::default(), reported: Default::default(), pending: Default::default() }
    }

    /// A call went to `feeds` feeds: its result waits for each to finish it, and for its files.
    pub fn routed(&self, id: u32, feeds: usize) {
        let mut p = self.pending.lock().unwrap();
        if p.len() >= MAX_PENDING {
            p.retain(|_, c| c.since.elapsed() < RESULT_AFTER);
        }
        p.insert(id, Pending { path: None, feeds, failures: Vec::new(), since: Instant::now() });
    }

    /// A feed is done with a call: it was streamed, or `failure` says why not.
    pub fn done(&self, id: u32, failure: Option<String>) {
        let mut p = self.pending.lock().unwrap();
        let Some(c) = p.get_mut(&id) else { return };
        c.feeds = c.feeds.saturating_sub(1);
        c.failures.extend(failure);
        self.settle(&mut p, id);
    }

    /// A call's files are written (at `path`): its result, once its feeds are done with it.
    pub fn concluded(&self, id: u32, path: &str) {
        let mut p = self.pending.lock().unwrap();
        match p.get_mut(&id) {
            Some(c) if c.path.is_none() => {
                c.path = Some(path.to_string());
                self.settle(&mut p, id);
            }
            // Another site's copy of a call already reported.
            _ => self.skipped(path, "another copy of this call was streamed"),
        }
    }

    /// A call that isn't streamed, on purpose.
    pub fn skipped(&self, path: &str, why: &str) {
        self.host.call_result(path, Outcome::Skipped, why, "");
    }

    fn settle(&self, p: &mut HashMap<u32, Pending>, id: u32) {
        if p.get(&id).is_some_and(|c| c.feeds == 0 && c.path.is_some()) {
            let c = p.remove(&id).expect("a call");
            let path = c.path.unwrap_or_default();
            if c.failures.is_empty() {
                self.host.call_result(&path, Outcome::Ok, "", "");
            } else {
                self.host.call_result(&path, Outcome::Failed, c.failures.join("; "), "");
            }
        }
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
    use crate::schedule::PREFILL;
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
        Settings { name: format!("County (127.0.0.1:{port}/feed)"), target, max_delay: Duration::from_secs(60), update_title: false, volume: 1.0 }
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
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into(), pan: Pan::Center });
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
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into(), pan: Pan::Center });
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
        feed.send(Msg::Start { id: 1, title: "Fire Dispatch (101)".into(), pan: Pan::Center });
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

    fn mix_pull(m: &mut Mix, n: usize, t: Instant) -> (Vec<i16>, Vec<i16>) {
        let (mut l, mut r) = (Vec::new(), Vec::new());
        m.pull(n, t, &mut l, &mut r);
        (l, r)
    }

    #[test]
    fn each_side_plays_its_own_calls_at_once() {
        let mut m = Mix::new(22050, Duration::from_secs(60), true, 1.0);
        let t = Instant::now();
        m.lane(Pan::Left).start(1, "left".into(), t);
        m.lane(Pan::Right).start(2, "right".into(), t);
        m.audio(1, 22050, &[100; 50], t);
        m.audio(2, 22050, &[200; 50], t);
        let (l, r) = mix_pull(&mut m, 50, t + PREFILL);
        assert_eq!((l, r), (vec![100; 50], vec![200; 50]));
    }

    #[test]
    fn the_center_is_in_both_ears() {
        let mut m = Mix::new(22050, Duration::from_secs(60), true, 1.0);
        let t = Instant::now();
        m.lane(Pan::Center).start(1, String::new(), t);
        m.lane(Pan::Left).start(2, String::new(), t);
        m.audio(1, 22050, &[30000; 10], t);
        m.audio(2, 22050, &[10000; 10], t);
        // Audio of a call it wasn't told of: the center.
        m.audio(3, 22050, &[7; 10], t);
        let (l, r) = mix_pull(&mut m, 10, t + PREFILL);
        // Mixed, without wrapping around.
        assert_eq!((l, r), (vec![i16::MAX; 10], vec![30000; 10]));
        assert!(m.lanes[1].knows(3));
    }

    #[test]
    fn a_side_queues_its_calls() {
        let mut m = Mix::new(22050, Duration::from_secs(60), true, 1.0);
        let t = Instant::now();
        m.lane(Pan::Left).start(1, String::new(), t);
        m.lane(Pan::Left).start(2, String::new(), t);
        m.audio(2, 22050, &[2; 10], t);
        m.audio(1, 22050, &[1; 10], t);
        let (l, r) = mix_pull(&mut m, 10, t + PREFILL);
        assert_eq!((l, r), (vec![1; 10], vec![0; 10]));
        // A call's end reaches whichever lane it's in.
        for lane in &mut m.lanes {
            lane.end(1);
        }
        let (l, _) = mix_pull(&mut m, 20000, t + PREFILL);
        assert!(l.contains(&2));
    }

    #[test]
    fn draining_keeps_the_sides_together() {
        let mut m = Mix::new(22050, Duration::from_secs(60), true, 1.0);
        let t = Instant::now();
        m.lane(Pan::Left).start(1, String::new(), t);
        m.audio(1, 22050, &[1; 100], t);
        m.lane(Pan::Right).start(2, String::new(), t);
        m.audio(2, 22050, &[2; 30], t);
        let (mut l, mut r) = (Vec::new(), Vec::new());
        m.drain(t, &mut l, &mut r);
        assert_eq!(l.len(), r.len());
        assert_eq!((l[99], r[29], r[30]), (1, 2, 0));
    }

    #[test]
    fn mono_is_one_lane() {
        let mut m = Mix::new(22050, Duration::from_secs(60), false, 1.0);
        let t = Instant::now();
        m.lane(Pan::Left).start(1, String::new(), t);
        m.lane(Pan::Right).start(2, String::new(), t);
        assert_eq!(m.lanes.len(), 1);
        m.audio(1, 22050, &[1; 10], t);
        let (l, r) = mix_pull(&mut m, 10, t + PREFILL);
        assert_eq!((l, r.len()), (vec![1; 10], 0));
    }

    #[test]
    fn the_volume_raises_and_clips() {
        let t = Instant::now();
        for stereo in [false, true] {
            let mut m = Mix::new(22050, Duration::from_secs(60), stereo, 3.0);
            m.audio(1, 22050, &[1000, -1000, 20000, -20000], t);
            let (l, r) = mix_pull(&mut m, 4, t + PREFILL);
            assert_eq!(l, [3000, -3000, i16::MAX, i16::MIN]);
            assert_eq!(r.is_empty(), !stereo);
        }
        let mut m = Mix::new(22050, Duration::from_secs(60), true, 0.5);
        m.lane(Pan::Center).start(1, String::new(), t);
        m.lane(Pan::Left).start(2, String::new(), t);
        m.audio(1, 22050, &[30000; 2], t);
        m.audio(2, 22050, &[10000; 2], t);
        // Summed before the volume: a quieter mix isn't clipped.
        assert_eq!(mix_pull(&mut m, 2, t + PREFILL), (vec![20000; 2], vec![15000; 2]));
    }

    #[test]
    fn a_stereo_feed_streams_stereo() {
        let server = fake::Server::ok();
        let (host, _) = testing::capture();
        let mut s = settings(server.port);
        (s.target.info.stereo, s.target.info.kbps) = (true, 32);
        let feed = Feed::start(s, Health::new(host)).unwrap();
        feed.send(Msg::Start { id: 1, title: String::new(), pan: Pan::Left });
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: tone(1600) });
        feed.send(Msg::End { id: 1 });
        feed.finish(soon());
        let c = &server.wait(|c| c.first().is_some_and(|c| c.closed))[0];
        let f = frames(&c.body).unwrap();
        assert!(f.iter().all(|f| f.kbps == 32 && !f.mono && !f.joint), "{:?}", f[0]);
    }

    #[test]
    fn titles_follow_the_call_that_began_last() {
        let server = fake::Server::ok();
        let (host, _) = testing::capture();
        let mut s = Settings { update_title: true, ..settings(server.port) };
        (s.target.info.stereo, s.target.info.kbps) = (true, 32);
        let feed = Feed::start(s, Health::new(host)).unwrap();
        feed.send(Msg::Start { id: 1, title: "Left".into(), pan: Pan::Left });
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: tone(16000) });
        server.wait(|c| c.iter().any(|c| c.head.contains("song=Left")));
        feed.send(Msg::Start { id: 2, title: "Right".into(), pan: Pan::Right });
        feed.send(Msg::Audio { id: 2, rate: 8000, samples: tone(800) });
        let c = server.wait(|c| c.iter().any(|c| c.head.contains("song=Right")));
        feed.finish(soon());
        assert!(c.iter().any(|c| c.head.contains("song=Right")), "the right call's title didn't follow");
    }

    #[test]
    fn a_result_once_the_feeds_are_done_and_the_files_written() {
        let (host, captured) = testing::capture();
        let h = Health::new(host);
        // Files first, then the feeds.
        h.routed(1, 2);
        h.concluded(1, "sys/1");
        h.done(1, None);
        assert!(captured.output().results().is_empty(), "one feed still playing it");
        h.done(1, None);
        // Feeds first, then the files; one feed failed.
        h.routed(2, 2);
        h.done(2, None);
        h.done(2, Some("B: dropped".into()));
        h.concluded(2, "sys/2");
        // Concluded again (another site's copy), and a call never routed.
        h.concluded(2, "sys/2b");
        h.skipped("sys/3", "not on any feed");
        let r: Vec<(String, Outcome, String)> = captured.output().results().into_iter().map(|(p, o, m, _)| (p, o, m)).collect();
        assert_eq!(
            r,
            [
                ("sys/1".into(), Outcome::Ok, String::new()),
                ("sys/2".into(), Outcome::Failed, "B: dropped".into()),
                ("sys/2b".into(), Outcome::Skipped, "another copy of this call was streamed".into()),
                ("sys/3".into(), Outcome::Skipped, "not on any feed".into()),
            ]
        );
    }

    #[test]
    fn a_call_played_while_the_server_is_down_failed() {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let (host, captured) = testing::capture();
        let health = Health::new(host);
        let feed = Feed::start(settings(port), health.clone()).unwrap();
        health.routed(1, 1);
        health.concluded(1, "sys/1");
        feed.send(Msg::Start { id: 1, title: String::new(), pan: Pan::Center });
        feed.send(Msg::Audio { id: 1, rate: 8000, samples: tone(800) });
        feed.send(Msg::End { id: 1 });
        feed.finish(soon());
        let r = captured.output().results();
        assert_eq!((r[0].1, r[0].2.contains("no connection")), (Outcome::Failed, true), "{r:?}");
    }

    #[test]
    fn a_format_mp3_doesnt_have_wont_start() {
        let (host, _) = testing::capture();
        let mut s = settings(9);
        s.target.info.kbps = 17;
        assert!(Feed::start(s, Health::new(host)).is_err());
    }
}
