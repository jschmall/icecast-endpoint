//! icecast-endpoint — a Trunk Recorder Pro plugin that streams the audio of
//! calls as they're recorded to an Icecast mount, such as a Broadcastify
//! feed: one continuous MP3 stream per feed, with silence between calls.

mod encode;
mod feed;
mod icecast;
#[cfg(test)]
mod live;
mod resample;
mod schedule;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use trunk_recorder_plugin::{AudioChunk, CallInfo, Host, Manifest, Plugin, Setup, TalkgroupFilter, topic};

use encode::Mp3;
use feed::{Feed, Health, Msg};
use icecast::{Protocol, StreamInfo, Target};

#[derive(Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(default)]
struct Config {
    /// Feeds
    ///
    /// The Icecast mounts to stream to, such as your Broadcastify feeds, and which calls each plays.
    feeds: Vec<FeedConfig>,
}

/// Feed
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct FeedConfig {
    /// Name
    ///
    /// What to call this feed in the log.
    name: String,
    /// Server
    ///
    /// The server's host name, as on your Broadcastify feed's Technical tab (for example audio9.broadcastify.com).
    #[schemars(extend("x-required" = true))]
    server: String,
    /// Port
    #[schemars(range(min = 1, max = 65535))]
    port: u16,
    /// Mount
    ///
    /// The mount point, as on the Technical tab (for example /abcd1234).
    #[schemars(extend("x-required" = true))]
    mount: String,
    /// Username
    ///
    /// Leave it as source unless your server says otherwise.
    username: String,
    /// Password
    ///
    /// The source password for the mount.
    #[schemars(extend("x-secret" = true, "x-required" = true))]
    password: String,
    /// Protocol
    ///
    /// How to connect. Automatic tries Icecast 2.4's PUT, then the older SOURCE.
    protocol: Protocol,
    /// Bitrate (kbps)
    ///
    /// Broadcastify feeds are 16 kbps. Other MP3 bitrates: 8, 24, 32, 40, 48, 64, 80, 96, 112, 128.
    #[schemars(range(min = 8, max = 320))]
    bitrate: u32,
    /// Sample rate (Hz)
    ///
    /// Broadcastify feeds are 22050 Hz. Other MP3 rates: 16000, 24000, 32000, 44100, 48000.
    #[schemars(range(min = 8000, max = 48000))]
    sample_rate: u32,
    /// System
    ///
    /// The system whose calls this feed plays. Leave it empty for every system.
    #[schemars(extend("x-system" = true))]
    system: String,
    /// Talkgroups
    ///
    /// The talkgroups this feed plays, by number. * matches any digits and ? any one (507*). Leave it empty for every talkgroup.
    #[serde(deserialize_with = "trunk_recorder_plugin::filter::patterns")]
    talkgroups: Vec<String>,
    /// Leave out talkgroups
    ///
    /// Talkgroups this feed never plays, the same way.
    #[serde(deserialize_with = "trunk_recorder_plugin::filter::patterns")]
    exclude_talkgroups: Vec<String>,
    /// Show the talkgroup as the title
    ///
    /// Set the stream's title to the talkgroup playing. Not every server takes titles from a source.
    update_title: bool,
    /// Stream name
    ///
    /// What listeners see the stream called. Leave it empty to use the feed's name.
    stream_name: String,
    /// Description
    description: String,
    /// Genre
    genre: String,
    /// Website
    #[schemars(url)]
    website: String,
    /// List publicly
    ///
    /// Ask the server to list the stream in its public directory.
    public: bool,
    /// Longest delay (seconds)
    ///
    /// Calls at the same time play one after another. When more than this much is waiting, the oldest waiting call is dropped.
    #[schemars(range(min = 5, max = 600))]
    max_delay_seconds: u32,
}

impl Default for FeedConfig {
    fn default() -> Self {
        FeedConfig {
            name: String::new(),
            server: String::new(),
            port: 80,
            mount: String::new(),
            username: "source".into(),
            password: String::new(),
            protocol: Protocol::Auto,
            bitrate: 16,
            sample_rate: 22050,
            system: String::new(),
            talkgroups: Vec::new(),
            exclude_talkgroups: Vec::new(),
            update_title: false,
            stream_name: String::new(),
            description: String::new(),
            genre: "Scanner".into(),
            website: String::new(),
            public: false,
            max_delay_seconds: 60,
        }
    }
}

/// The password is never shown.
impl std::fmt::Debug for FeedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FeedConfig")
            .field("name", &self.name)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("mount", &self.mount)
            .field("username", &self.username)
            .field("password", &"***")
            .finish_non_exhaustive()
    }
}

/// A feed, and the calls it plays.
struct Route {
    feed: Feed,
    /// The system it plays (empty: all).
    system: String,
    filter: TalkgroupFilter,
    /// A system's own feed (it overrides the feeds): for it alone.
    only: Option<String>,
    /// The systems with feeds of their own, which the plugin's feeds aren't for.
    not: Arc<HashSet<String>>,
}

impl Route {
    fn wants(&self, short_name: &str, talkgroups: &[u32]) -> bool {
        let system = match &self.only {
            Some(o) => o == short_name,
            None => !self.not.contains(short_name),
        };
        system && (self.system.is_empty() || self.system == short_name) && talkgroups.iter().any(|&t| self.filter.passes(t))
    }
}

/// Calls tracked at most; beyond this, ones not heard from in a while are forgotten.
const MAX_TRACKED: usize = 1000;
const FORGET_AFTER: Duration = Duration::from_secs(600);

struct IcecastEndpoint {
    routes: Vec<Route>,
    /// The calls going now (each once, however many sites hear it).
    calls: HashMap<u32, CallInfo>,
    /// The calls routed, by id: the feeds playing each (maybe none), and when last heard.
    playing: HashMap<u32, (Vec<usize>, Instant)>,
}

impl Plugin for IcecastEndpoint {
    type Config = Config;
    type SystemConfig = trunk_recorder_plugin::NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Icecast Endpoint".into(),
            subscribe: vec![topic::AUDIO.into(), topic::CALL_START.into(), topic::CALL_END.into()],
            // The id, version, description, … come from Cargo.toml.
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, Self::SystemConfig>) -> Result<Self, String> {
        // Systems can override the feeds: theirs are for them alone.
        let own: Vec<(&str, &Vec<FeedConfig>)> =
            setup.systems.iter().filter(|s| s.settings.feeds != setup.config.feeds).map(|s| (s.short_name.as_str(), &s.settings.feeds)).collect();
        if setup.config.feeds.is_empty() && own.iter().all(|(_, v)| v.is_empty()) {
            return Err("Add a feed: the Icecast server and mount to stream to, such as your Broadcastify feed's.".into());
        }
        let not: Arc<HashSet<String>> = Arc::new(own.iter().map(|(n, _)| n.to_string()).collect());
        // Check every feed before starting any.
        let mut checked = Vec::new();
        let mut counts: HashMap<Option<&str>, usize> = HashMap::new();
        let all = setup.config.feeds.iter().map(|f| (None, f));
        let theirs = own.iter().flat_map(|(n, v)| v.iter().map(move |f| (Some(*n), f)));
        for (only, f) in all.chain(theirs) {
            let i = *counts.entry(only).and_modify(|n| *n += 1).or_insert(1);
            let which = match (only, f.name.trim()) {
                (Some(n), "") => format!("{n}'s feed {i}"),
                (Some(n), name) => format!("{n}'s feed {name}"),
                (None, "") => format!("Feed {i}"),
                (None, name) => format!("Feed {name}"),
            };
            let target = f.target().map_err(|e| format!("{which}: {e}"))?;
            if checked
                .iter()
                .any(|(_, _, t, _): &(Option<&str>, &FeedConfig, Target, String)| t.host == target.host && t.port == target.port && t.mount == target.mount)
            {
                return Err(format!("{which}: another feed streams to the same mount ({target}); a mount takes one source."));
            }
            checked.push((only, f, target, which));
        }
        let health = Health::new(host.clone());
        let mut routes = Vec::new();
        for (only, f, target, which) in checked {
            if !f.system.is_empty() && setup.system_named(&f.system).is_none() {
                host.warn(format!("{which}: there's no system called {}", f.system));
            }
            let filter = TalkgroupFilter::new(&f.talkgroups, &f.exclude_talkgroups);
            let name = if f.name.trim().is_empty() { target.to_string() } else { format!("{} ({target})", f.name.trim()) };
            let of = match (only, f.system.as_str()) {
                (Some(n), _) | (None, n) if !n.is_empty() => format!(" of {n}"),
                _ => String::new(),
            };
            let tgs = if filter.is_empty() { "every talkgroup".to_string() } else { filter.describe() };
            host.info(format!("feed {name}: {tgs}{of}, {} kbps at {} Hz", f.bitrate, f.sample_rate));
            let settings = feed::Settings { target, name, max_delay: Duration::from_secs(f.max_delay_seconds as u64), update_title: f.update_title };
            let feed = Feed::start(settings, health.clone()).map_err(|e| format!("{which}: {e}"))?;
            routes.push(Route { feed, system: f.system.clone(), filter, only: only.map(String::from), not: not.clone() });
        }
        Ok(IcecastEndpoint { routes, calls: HashMap::new(), playing: HashMap::new() })
    }

    fn call_start(&mut self, call: CallInfo) {
        self.forget_old();
        // A call not recorded here has no audio of its own; another site's copy may,
        // and is routed when its audio comes (as this call).
        if call.recording {
            self.route(call.id, &call.short_name, &talkgroups(&call), title(&call), skip(&call));
        }
        self.calls.insert(call.id, call);
    }

    fn audio(&mut self, chunk: AudioChunk) {
        // A call heard on several sites plays from whichever copy has it: one that wasn't
        // told to start is the call that was (the same system and talkgroup).
        let id = if self.playing.contains_key(&chunk.call_id) || self.calls.contains_key(&chunk.call_id) {
            chunk.call_id
        } else {
            self.calls.values().find(|c| c.short_name == chunk.short_name && c.talkgroup == chunk.talkgroup).map_or(chunk.call_id, |c| c.id)
        };
        if !self.playing.contains_key(&id) {
            let (tgs, t, s) = match self.calls.get(&id) {
                Some(c) => (talkgroups(c), title(c), skip(c)),
                None => (vec![chunk.talkgroup], format!("Talkgroup {}", chunk.talkgroup), false),
            };
            self.route(id, &chunk.short_name, &tgs, t, s);
        }
        let Some((routes, heard)) = self.playing.get_mut(&id) else { return };
        *heard = Instant::now();
        if routes.is_empty() {
            return;
        }
        let samples = chunk.samples();
        for &r in routes.iter() {
            self.routes[r].feed.send(Msg::Audio { id, rate: chunk.sample_rate, samples: samples.clone() });
        }
    }

    fn call_end(&mut self, call: CallInfo) {
        self.calls.remove(&call.id);
        if let Some((routes, _)) = self.playing.remove(&call.id) {
            for r in routes {
                self.routes[r].feed.send(Msg::End { id: call.id });
            }
        }
    }

    fn shutdown(&mut self, grace: Duration) {
        let deadline = Instant::now() + grace;
        for r in &mut self.routes {
            r.feed.close();
        }
        for r in self.routes.drain(..) {
            r.feed.finish(deadline);
        }
    }
}

impl IcecastEndpoint {
    /// Start a call on the feeds that play it, and remember which.
    fn route(&mut self, id: u32, short_name: &str, talkgroups: &[u32], title: String, skip: bool) {
        if self.playing.contains_key(&id) {
            return;
        }
        let routes: Vec<usize> = if skip { Vec::new() } else { (0..self.routes.len()).filter(|&i| self.routes[i].wants(short_name, talkgroups)).collect() };
        for &r in &routes {
            self.routes[r].feed.send(Msg::Start { id, title: title.clone() });
        }
        self.playing.insert(id, (routes, Instant::now()));
    }

    /// Calls whose end never came (the recorder drops events for a plugin that falls behind).
    fn forget_old(&mut self) {
        if self.calls.len() + self.playing.len() < MAX_TRACKED {
            return;
        }
        let now = Instant::now();
        let routes = &self.routes;
        self.playing.retain(|id, (r, heard)| {
            let keep = now.duration_since(*heard) < FORGET_AFTER;
            if !keep {
                for &i in r.iter() {
                    routes[i].feed.send(Msg::End { id: *id });
                }
            }
            keep
        });
        let playing = &self.playing;
        self.calls.retain(|id, _| playing.contains_key(id));
    }
}

impl FeedConfig {
    /// Where it streams, and how; errors say what to fix.
    fn target(&self) -> Result<Target, String> {
        let mut host = self.server.trim();
        if host.is_empty() {
            return Err("add the server it streams to".into());
        }
        if host.to_ascii_lowercase().starts_with("https://") {
            return Err(format!("{host}: https isn't supported; use the server's plain (http) port"));
        }
        if host.len() > 7 && host[..7].eq_ignore_ascii_case("http://") {
            host = &host[7..];
        }
        let host = host.trim_end_matches('/');
        if host.is_empty() || host.contains(['/', ' ', '@']) || host.contains("://") {
            return Err(format!("\"{}\" isn't a host name; put just the server's name in Server, and the mount in Mount", self.server.trim()));
        }
        if self.port == 0 {
            return Err("the port can't be 0".into());
        }
        let mount = self.mount.trim().trim_start_matches('/');
        if mount.is_empty() {
            return Err("add the mount it streams to".into());
        }
        if self.password.is_empty() {
            return Err("add the mount's source password".into());
        }
        if !(5..=600).contains(&self.max_delay_seconds) {
            return Err(format!("the longest delay is {} seconds; make it from 5 to 600", self.max_delay_seconds));
        }
        Mp3::new(self.sample_rate, self.bitrate)?;
        let user = if self.username.trim().is_empty() { "source" } else { self.username.trim() };
        let name = if self.stream_name.trim().is_empty() { self.name.trim() } else { self.stream_name.trim() };
        Ok(Target {
            host: host.to_string(),
            port: self.port,
            mount: format!("/{mount}"),
            user: user.to_string(),
            password: self.password.clone(),
            protocol: self.protocol,
            info: StreamInfo {
                name: name.to_string(),
                description: self.description.trim().to_string(),
                genre: self.genre.trim().to_string(),
                url: self.website.trim().to_string(),
                public: self.public,
                rate: self.sample_rate,
                kbps: self.bitrate,
            },
        })
    }
}

/// The call's talkgroup, then those patched with it.
fn talkgroups(c: &CallInfo) -> Vec<u32> {
    let mut v = vec![c.talkgroup];
    v.extend(c.patched_talkgroups.iter().filter(|&&t| t != c.talkgroup));
    v
}

/// What the stream's title says while the call plays.
fn title(c: &CallInfo) -> String {
    match c.talkgroup_tag.trim() {
        "" => format!("Talkgroup {}", c.talkgroup),
        tag => format!("{tag} ({})", c.talkgroup),
    }
}

/// Encrypted calls have nothing to hear, unless the system had the key.
fn skip(c: &CallInfo) -> bool {
    c.encrypted && !c.decrypted
}

fn main() {
    trunk_recorder_plugin::run::<IcecastEndpoint>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::icecast::fake;
    use serde_json::{Value, json};
    use trunk_recorder_plugin::{EXIT_CONFIG, HostMessage, SystemInfo, testing};

    fn call(id: u32, tg: u32) -> CallInfo {
        CallInfo { id, short_name: "sys1".into(), talkgroup: tg, talkgroup_tag: format!("TG {tg}"), recording: true, units: vec![1234], ..Default::default() }
    }

    /// 0.2 s of a call's audio, in the recorder's 20 ms chunks.
    fn audio(id: u32, tg: u32) -> Vec<HostMessage> {
        (0..10).map(|_| HostMessage::Audio(AudioChunk { short_name: "sys1".into(), ..AudioChunk::new(id, 0, tg, 8000, &[1000; 160]) })).collect()
    }

    fn feed(port: u16, extra: Value) -> Value {
        let mut f = json!({ "name": "County", "server": "127.0.0.1", "port": port, "mount": "/feed", "password": "hackme" });
        f.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        f
    }

    fn hello(feeds: Value) -> HostMessage {
        HostMessage::Hello(testing::hello(&testing::temp_dir("icecast-endpoint"), json!({ "feeds": feeds })))
    }

    fn run(messages: Vec<HostMessage>) -> testing::Output {
        testing::run::<IcecastEndpoint>(messages)
    }

    /// Calls the feeds played, from the last metrics.
    fn played(out: &testing::Output) -> u64 {
        out.metrics().last().expect("metrics").extra["callsPlayed"].as_u64().unwrap()
    }

    /// A call, start to end, with its audio.
    fn whole(c: CallInfo) -> Vec<HostMessage> {
        let mut m = vec![HostMessage::CallStart(c.clone())];
        m.extend(audio(c.id, c.talkgroup));
        m.push(HostMessage::CallEnd(c));
        m
    }

    #[test]
    fn the_settings_form() {
        let c = trunk_recorder_plugin::describe::<IcecastEndpoint>().config.unwrap();
        let f = &c["properties"]["feeds"]["items"];
        assert_eq!(f["title"], "Feed");
        assert_eq!(
            f["x-order"],
            json!([
                "name",
                "server",
                "port",
                "mount",
                "username",
                "password",
                "protocol",
                "bitrate",
                "sampleRate",
                "system",
                "talkgroups",
                "excludeTalkgroups",
                "updateTitle",
                "streamName",
                "description",
                "genre",
                "website",
                "public",
                "maxDelaySeconds"
            ])
        );
        let p = &f["properties"];
        assert_eq!(p["password"]["x-secret"], true);
        assert_eq!(p["system"]["x-system"], true);
        assert_eq!(p["bitrate"]["default"], 16);
        assert_eq!(p["sampleRate"]["default"], 22050);
        assert_eq!(p["port"]["default"], 80);
        assert_eq!(p["username"]["default"], "source");
        assert_eq!(p["maxDelaySeconds"]["default"], 60);
        assert_eq!(f["required"], json!(["server", "mount", "password"]));
    }

    #[test]
    fn the_manifest() {
        let m = trunk_recorder_plugin::describe::<IcecastEndpoint>();
        assert_eq!(m.subscribe, ["audio", "call.start", "call.end"]);
        assert!(m.system_config.is_none());
    }

    #[test]
    fn bad_settings_say_what_to_fix() {
        let cases = [
            (json!([]), "Add a feed"),
            (json!([feed(8000, json!({ "server": "" }))]), "server"),
            (json!([feed(8000, json!({ "server": "https://x.example" }))]), "https"),
            (json!([feed(8000, json!({ "server": "x.example/feed" }))]), "host name"),
            (json!([feed(8000, json!({ "mount": " " }))]), "mount"),
            (json!([feed(8000, json!({ "password": "" }))]), "password"),
            (json!([feed(0, json!({}))]), "port"),
            (json!([feed(8000, json!({ "bitrate": 17 }))]), "17 kbps"),
            (json!([feed(8000, json!({ "sampleRate": 11000 }))]), "11000"),
            (json!([feed(8000, json!({ "maxDelaySeconds": 1 }))]), "delay"),
            (json!([feed(8000, json!({})), feed(8000, json!({ "name": "Again", "mount": "feed" }))]), "same mount"),
        ];
        for (feeds, says) in cases {
            let out = run(vec![hello(feeds.clone())]);
            assert_eq!(out.exit_code, EXIT_CONFIG, "{feeds}");
            let (_, msg) = out.status().unwrap();
            assert!(msg.to_lowercase().contains(&says.to_lowercase()), "{feeds}: {msg}");
            assert!(!msg.contains("hackme"));
        }
    }

    #[test]
    fn settings_that_are_good() {
        let server = fake::Server::ok();
        let out = run(vec![hello(json!([feed(server.port, json!({ "server": "http://127.0.0.1/" }))]))]);
        assert!(out.ready(), "{:?}", out.messages);
    }

    #[test]
    fn the_streams_details_go_to_the_server() {
        let server = fake::Server::ok();
        let f = feed(
            server.port,
            json!({
                "mount": "scanner", "username": "me", "protocol": "source", "bitrate": 24, "sampleRate": 16000,
                "streamName": "County Fire", "description": "Dispatch", "genre": "Fire", "website": "https://example.com", "public": true
            }),
        );
        let out = run(vec![hello(json!([f]))]);
        assert!(out.ready());
        let head = &server.wait(|c| !c.is_empty())[0].head;
        assert!(head.starts_with("SOURCE /scanner HTTP/1.0\r\n"), "{head}");
        // me:hackme
        assert!(head.contains("Authorization: Basic bWU6aGFja21l\r\n"), "{head}");
        for h in [
            "ice-name: County Fire",
            "ice-description: Dispatch",
            "ice-genre: Fire",
            "ice-url: https://example.com",
            "ice-public: 1",
            "ice-audio-info: channels=1;samplerate=16000;bitrate=24",
        ] {
            assert!(head.contains(&format!("{h}\r\n")), "{h}: {head}");
        }
    }

    #[test]
    fn the_feeds_name_is_the_stream_name_by_default() {
        let server = fake::Server::ok();
        run(vec![hello(json!([feed(server.port, json!({}))]))]);
        assert!(server.wait(|c| !c.is_empty())[0].head.contains("ice-name: County\r\n"));
    }

    #[test]
    fn plays_calls() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(whole(call(1, 101)));
        m.extend(whole(call(2, 202)));
        let out = run(m);
        assert_eq!(played(&out), 2, "{:?}", out.messages);
        assert!(server.wait(|c| c.first().is_some_and(|c| c.closed))[0].body.len() > 100);
    }

    #[test]
    fn talkgroups_to_play_and_to_leave_out() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["1*"], "excludeTalkgroups": [102] }))]))];
        for (id, tg) in [(1, 101), (2, 102), (3, 202), (4, 103)] {
            m.extend(whole(call(id, tg)));
        }
        assert_eq!(played(&run(m)), 2);
    }

    #[test]
    fn a_patched_call_plays_on_a_feed_for_any_of_its_talkgroups() {
        let server = fake::Server::ok();
        let mut c = call(1, 101);
        c.patched_talkgroups = vec![101, 202];
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["202"] }))]))];
        m.extend(whole(c));
        assert_eq!(played(&run(m)), 1);
    }

    #[test]
    fn other_systems_are_left_out() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "system": "elsewhere" }))]))];
        m.extend(whole(call(1, 101)));
        let out = run(m);
        assert_eq!(played(&out), 0);
        assert!(out.logs().iter().any(|(_, l)| l.contains("no system called elsewhere")), "{:?}", out.logs());
    }

    #[test]
    fn encrypted_calls_are_left_out() {
        let server = fake::Server::ok();
        let mut c = call(1, 101);
        c.encrypted = true;
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(whole(c));
        assert_eq!(played(&run(m)), 0);
    }

    /// A call heard on several sites starts once, as its first site's copy (here
    /// not recorded); its audio can come from another site's copy, with another id.
    #[test]
    fn audio_from_another_sites_copy() {
        let server = fake::Server::ok();
        let mut first = call(1, 101);
        first.recording = false;
        let mut m = vec![hello(json!([feed(server.port, json!({}))])), HostMessage::CallStart(first.clone())];
        m.extend(audio(9, 101));
        m.push(HostMessage::CallEnd(first));
        // One call, played and ended.
        assert_eq!(played(&run(m)), 1);
    }

    #[test]
    fn audio_without_a_start_still_plays() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["101"] }))]))];
        m.extend(audio(7, 101));
        m.extend(audio(8, 202));
        let out = run(m);
        // Not ended, so not counted as played, but its audio went and the other's didn't.
        assert_eq!(out.metrics().last().unwrap().extra["callsDropped"], 0);
        assert!(out.ready());
    }

    #[test]
    fn calls_not_recorded_dont_hold_up_the_feed() {
        let server = fake::Server::ok();
        let mut skipped = call(1, 101);
        skipped.recording = false;
        let mut m = vec![hello(json!([feed(server.port, json!({}))])), HostMessage::CallStart(skipped.clone()), HostMessage::CallEnd(skipped)];
        m.extend(whole(call(2, 202)));
        // Only the recorded call counts.
        assert_eq!(played(&run(m)), 1);
    }

    #[test]
    fn each_feed_its_own_calls() {
        let (a, b) = (fake::Server::ok(), fake::Server::ok());
        let mut m = vec![hello(json!([feed(a.port, json!({ "talkgroups": ["101"] })), feed(b.port, json!({ "name": "Other", "talkgroups": ["202"] }))]))];
        m.extend(whole(call(1, 101)));
        m.extend(whole(call(2, 202)));
        m.extend(whole(call(3, 101)));
        let out = run(m);
        assert_eq!(played(&out), 3);
        assert_eq!(out.metrics().last().unwrap().endpoints.len(), 2);
    }

    /// A system with feeds of its own: they're for it alone, and the plugin's aren't for it.
    #[test]
    fn a_system_overrides_the_feeds() {
        let (all, own) = (fake::Server::ok(), fake::Server::ok());
        let mut h = testing::hello(&testing::temp_dir("icecast-endpoint"), json!({ "feeds": [feed(all.port, json!({}))] }));
        h.systems.push(SystemInfo {
            index: 1,
            short_name: "sys2".into(),
            kind: "p25".into(),
            settings: json!({ "feeds": [feed(own.port, json!({ "name": "Own" }))] }),
            ..Default::default()
        });
        let sys2 = CallInfo { short_name: "sys2".into(), system: 1, ..call(2, 202) };
        let mut m = vec![HostMessage::Hello(h)];
        m.extend(whole(call(1, 101)));
        m.push(HostMessage::CallStart(sys2.clone()));
        m.extend((0..10).map(|_| HostMessage::Audio(AudioChunk { short_name: "sys2".into(), ..AudioChunk::new(2, 1, 202, 8000, &[1000; 160]) })));
        m.push(HostMessage::CallEnd(sys2));
        let out = run(m);
        assert!(out.ready(), "{:?}", out.messages);
        assert_eq!(out.metrics().last().unwrap().endpoints.len(), 2);
        // One call each.
        assert_eq!(played(&out), 2);
        assert!(all.wait(|c| c.first().is_some_and(|c| c.closed))[0].body.len() > 100);
        assert!(own.wait(|c| c.first().is_some_and(|c| c.closed))[0].body.len() > 100);
    }

    #[test]
    fn the_password_never_shows() {
        let server = fake::Server::start(|_| "HTTP/1.0 401 Authentication Required\r\n\r\n".into());
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(whole(call(1, 101)));
        let out = run(m);
        assert!(out.ready());
        let all = format!("{:?}", out.messages);
        assert!(all.contains("wrong username or password"), "{all}");
        assert!(!all.contains("hackme"), "{all}");
        let f: FeedConfig = serde_json::from_value(feed(1, json!({}))).unwrap();
        assert!(!format!("{f:?}").contains("hackme"));
    }

    #[test]
    fn shutdown_keeps_to_its_grace() {
        let server = fake::Server::start(|_| {
            std::thread::sleep(Duration::from_secs(30));
            String::new()
        });
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(whole(call(1, 101)));
        m.push(HostMessage::Shutdown(trunk_recorder_plugin::protocol::Shutdown { grace_s: 1.0 }));
        let t = std::time::Instant::now();
        run(m);
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    }
}
