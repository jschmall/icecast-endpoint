//! icecast-endpoint — a Trunk Recorder Pro plugin that streams the audio of
//! calls as they're recorded to an Icecast mount, such as a Broadcastify
//! feed: one continuous MP3 stream per feed, with silence between calls.

mod broadcastify;
mod encode;
mod feed;
mod icecast;
#[cfg(test)]
mod live;
mod resample;
mod schedule;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use trunk_recorder_plugin::filter::{TalkgroupMode, patterns, talkgroup_list};
use trunk_recorder_plugin::{Action, AudioChunk, CallInfo, ConcludedCall, Host, Manifest, Plugin, Reply, Setup, State, TalkgroupFilter, Talkgroups, topic};

use broadcastify::BcfyFeed;
use encode::Mp3;
use feed::{Feed, Health, Msg, Pan};
use icecast::{Protocol, StreamInfo, Target};

#[derive(Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(default)]
#[schemars(extend("x-actions" = [{
    "id": LOAD_FEEDS,
    "label": "Load my Broadcastify feeds",
    "hint": "Logs in with the Broadcastify username and password above and lists your account's feeds under Your Broadcastify feeds, each with Use this feed. The password isn't kept. The recorder has to be recording."
}]))]
struct Config {
    /// Feeds
    ///
    /// The Icecast mounts to stream to, such as your Broadcastify feeds, and which calls each plays.
    #[serde(deserialize_with = "feeds")]
    feeds: Vec<FeedConfig>,
    /// Broadcastify username
    ///
    /// To stream to a feed on your Broadcastify account without copying its details: your Broadcastify username and password, then Load my Broadcastify feeds. The password is cleared once your feeds are loaded.
    #[serde(rename = "broadcastifyUsername")]
    broadcastify_username: String,
    /// Broadcastify password
    #[serde(rename = "broadcastifyPassword")]
    #[schemars(extend("x-secret" = true))]
    broadcastify_password: String,
    /// Your Broadcastify feeds
    ///
    /// Your account's feeds, listed by Load my Broadcastify feeds: open the one to stream and press Use this feed.
    #[serde(rename = "broadcastifyFeeds")]
    #[schemars(extend("readOnly" = true))]
    broadcastify_feeds: Vec<Listed>,
    /// Where an account's feeds come from in tests (a mock server). Not in release builds: the
    /// login only ever goes to Broadcastify.
    #[cfg(test)]
    #[serde(rename = "broadcastifyApi", skip_serializing_if = "String::is_empty")]
    #[schemars(skip)]
    broadcastify_api: String,
}

/// The button that lists a Broadcastify account's feeds.
const LOAD_FEEDS: &str = "broadcastify";
/// The button on a feed listed from the account, which adds it (or fills in the one on its mount).
const USE_FEED: &str = "broadcastify-feed";
/// How long a press waits for what was typed just before it to be saved: the form saves 300 ms
/// after the last change, and a save of the plugin's own made first would turn it down.
const TYPED_WAIT: Duration = Duration::from_secs(if cfg!(test) { 1 } else { 3 });

/// One of the account's feeds, as listed in the settings (the source password isn't).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(extend(
    "x-title" = "name",
    "x-summary" = ["Feed {id}", "{server}:{port}{mount}"],
    "x-actions" = [{
        "id": USE_FEED,
        "label": "Use this feed",
        "hint": "Adds this feed, or fills in the one already streaming to its mount, with its server, port, mount and password."
    }]
))]
struct Listed {
    /// Feed ID
    id: u32,
    /// Name
    name: String,
    /// Server
    server: String,
    /// Port
    port: u16,
    /// Mount
    mount: String,
}

impl Listed {
    fn of(f: &BcfyFeed) -> Listed {
        Listed { id: f.id, name: f.name.trim().to_string(), server: f.host.clone(), port: f.port, mount: f.mount() }
    }
}

/// The Broadcastify login as last saved (the form saves what's typed as it's typed),
/// and a signal for each new save.
type Typed = Arc<(Mutex<(String, String)>, Condvar)>;

/// An account's feeds as last looked up, and the settings as they were then: kept in memory only
/// (they have the feeds' source passwords), so a feed on the list can be filled in.
struct Account {
    feeds: Vec<BcfyFeed>,
    settings: Value,
}

/// (Without the source passwords.)
impl std::fmt::Debug for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Account").field("feeds", &self.feeds.iter().map(|f| f.id).collect::<Vec<_>>()).finish_non_exhaustive()
    }
}

/// Replies waiting for their settings to be saved: by the save's request, the reply, what to say
/// once saved, and what to say if they can't be.
type Pending = Arc<Mutex<HashMap<u64, (Reply, String, String)>>>;

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
    /// Broadcastify recommends 16 kbps (32 in stereo). Other MP3 bitrates: 8, 24, 32, 40, 48, 64, 80, 96, 112, 128.
    #[schemars(range(min = 8, max = 320))]
    bitrate: u32,
    /// Sample rate (Hz)
    ///
    /// Broadcastify recommends 22050 Hz. Other MP3 rates: 16000, 24000, 32000, 44100, 48000.
    #[schemars(range(min = 8000, max = 48000))]
    sample_rate: u32,
    /// Volume
    ///
    /// How loud to play the calls: 1 is as recorded, 2 twice as loud, 0.5 half. Loud parts are clipped.
    #[schemars(range(min = 0.1, max = 10.0))]
    volume: f64,
    /// System
    ///
    /// The system whose calls this feed plays. Leave it empty for every system.
    #[schemars(extend("x-system" = true))]
    system: String,
    // The talkgroups it plays: the recorder's Talkgroups menu (every one, only some, all but
    // some) and the lists it asks for, as every plugin has it.
    #[serde(flatten)]
    talkgroups: Talkgroups,
    /// Talkgroups on the left
    ///
    /// Talkgroups heard in the left ear: numbers, ranges like 1000-1099, and patterns like 507*. Any here or on the right makes the feed stereo (Broadcastify recommends 32 kbps for stereo). With "Only some", these play too.
    #[serde(deserialize_with = "patterns")]
    #[schemars(with = "Vec<String>", extend("x-talkgroups" = true))]
    left_talkgroups: Vec<String>,
    /// Talkgroups on the right
    ///
    /// Talkgroups heard in the right ear. Others play in both ears. The left, the center and the right each play their calls one at a time, and the three play at once.
    #[serde(deserialize_with = "patterns")]
    #[schemars(with = "Vec<String>", extend("x-talkgroups" = true))]
    right_talkgroups: Vec<String>,
    /// Show the talkgroup as the title
    ///
    /// Set the stream's title to the talkgroup playing. Not every server takes titles from a source.
    update_title: bool,
    /// Title
    ///
    /// What the title says: {alias} (the talkgroup's alpha tag), {talkgroup} (its number) and {system}. {alias} alone leaves off the number. A talkgroup with no alias shows as Talkgroup 101.
    title_format: String,
    /// Title when quiet
    ///
    /// The title once nothing has played for a while. Leave it empty to clear the title, or say what the feed is (Scanning).
    idle_title: String,
    /// Quiet after (seconds)
    ///
    /// How long after the last call the title changes to the one above. 0 keeps the last call's title.
    #[schemars(range(min = 0, max = 3600))]
    idle_seconds: u32,
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
    /// Calls at the same time (on the same side, for a stereo feed) play one after another. When more than this much is waiting, the oldest waiting call is dropped.
    #[schemars(range(min = 5, max = 600))]
    max_delay_seconds: u32,
    /// 0.4.0 and earlier's Talkgroups list: read, not shown or written. It applies only to a feed
    /// saved before the menu (no `talkgroupMode`).
    #[serde(rename = "talkgroups", deserialize_with = "patterns", skip_serializing)]
    #[schemars(skip)]
    old_talkgroups: Vec<String>,
    /// 0.4.0 and earlier's Leave out talkgroups list, the same way.
    #[serde(rename = "excludeTalkgroups", deserialize_with = "patterns", skip_serializing)]
    #[schemars(skip)]
    old_exclude_talkgroups: Vec<String>,
    /// The feed was saved with a Talkgroups menu choice (`talkgroupMode`): set as the settings are read.
    #[serde(skip)]
    #[schemars(skip)]
    menu: bool,
}

/// What a feed plays, and where.
struct Filters {
    plays: TalkgroupFilter,
    left: TalkgroupFilter,
    right: TalkgroupFilter,
    /// The lists saved by 0.4.0 or earlier apply (the menu was never chosen).
    old: bool,
}

/// The feeds, each told whether it was saved with a Talkgroups menu choice: the menu's own
/// default (every talkgroup) can't say so.
fn feeds<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<FeedConfig>, D::Error> {
    let raw: Option<Vec<serde_json::Value>> = Option::deserialize(d)?;
    raw.unwrap_or_default()
        .into_iter()
        .map(|v| {
            let menu = v.get("talkgroupMode").is_some_and(|m| !m.is_null());
            let mut f: FeedConfig = serde_json::from_value(v).map_err(serde::de::Error::custom)?;
            f.menu = menu;
            Ok(f)
        })
        .collect()
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
            volume: 1.0,
            system: String::new(),
            talkgroups: Talkgroups::default(),
            left_talkgroups: Vec::new(),
            right_talkgroups: Vec::new(),
            update_title: false,
            title_format: TITLE_FORMAT.into(),
            idle_title: String::new(),
            idle_seconds: 5,
            stream_name: String::new(),
            description: String::new(),
            genre: "Scanner".into(),
            website: String::new(),
            public: false,
            max_delay_seconds: 60,
            old_talkgroups: Vec::new(),
            old_exclude_talkgroups: Vec::new(),
            menu: true,
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
    /// The talkgroups heard on the left and on the right (empty: none).
    left: TalkgroupFilter,
    right: TalkgroupFilter,
    /// A system's own feed (it overrides the feeds): for it alone.
    only: Option<String>,
    /// The systems with feeds of their own, which the plugin's feeds aren't for.
    not: Arc<HashSet<String>>,
    /// What its title says ([`Title::render`]).
    title_format: String,
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
/// Another site's copy of a call started within this many seconds of it.
const TWIN_WITHIN: f64 = 5.0;

/// Audio of a call not told of yet waits this long for its start, or until there's this much of it.
const EARLY_WAIT: Duration = Duration::from_millis(500);
const EARLY_MOST_MS: usize = 3000;

/// Audio that came before its call's start. The recorder sends a block's audio before its calls'
/// starts, so a transmission on a channel it kept open (conversation mode off) begins this way.
struct Early {
    since: Instant,
    chunks: Vec<AudioChunk>,
    ms: usize,
}

/// Which feeds a call went to, kept until its files are written (`call.concluded`).
struct Routed {
    short_name: String,
    talkgroup: u32,
    /// Unix seconds.
    start: f64,
    feeds: usize,
    encrypted: bool,
    at: Instant,
}

struct IcecastEndpoint {
    routes: Vec<Route>,
    /// The calls going now (each once, however many sites hear it).
    calls: HashMap<u32, CallInfo>,
    /// The calls routed, by id: the feeds playing each (maybe none), and when last heard.
    playing: HashMap<u32, (Vec<usize>, Instant)>,
    /// Audio waiting for its call's start, by the id it came with.
    early: HashMap<u32, Early>,
    /// The calls routed, until their files are written.
    routed: HashMap<u32, Routed>,
    health: Health,
    host: Host,
    /// Where a Broadcastify account's feeds come from.
    api: String,
    /// The Broadcastify buttons: the presses being answered, and their replies waiting on a save.
    workers: Vec<JoinHandle<()>>,
    pending: Pending,
    typed: Typed,
    account: Arc<Mutex<Option<Account>>>,
    /// Lookups of the account's feeds under way (a feed on the list waits for them).
    looking: Looking,
    /// The feeds as set (the plugin's, and each system's own): new settings that leave them as
    /// they are don't restart them.
    feeds_now: FeedsNow,
}

/// A button's work, on a thread of its own: the settings to save, what to say once saved, and
/// what to say if they can't be; or what to tell the user.
type Work = Box<dyn FnOnce() -> Result<(Value, String, String), String> + Send>;

/// How many lookups are under way, and a signal as each ends.
type Looking = Arc<(Mutex<usize>, Condvar)>;

type FeedsNow = (Vec<FeedConfig>, Vec<(String, Vec<FeedConfig>)>);

fn feeds_now(setup: &Setup<Config, trunk_recorder_plugin::NoConfig>) -> FeedsNow {
    (setup.config.feeds.clone(), setup.systems.iter().map(|s| (s.short_name.clone(), s.settings.feeds.clone())).collect())
}

/// What's typed, as the settings have it.
fn typed_of(c: &Config) -> (String, String) {
    (c.broadcastify_username.clone(), c.broadcastify_password.clone())
}

/// What's typed, as a button's value has it.
fn typed_in(v: &Value) -> (String, String) {
    let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    (s("broadcastifyUsername"), s("broadcastifyPassword"))
}

/// Wait (a few seconds at most) until what's typed in `want` has been saved: a save of the
/// plugin's own, made first, would turn the form's down ("changed elsewhere meanwhile").
fn wait_for_typed(typed: &Typed, want: &(String, String)) {
    let (m, cv) = &**typed;
    let g = m.lock().unwrap();
    let _ = cv.wait_timeout_while(g, TYPED_WAIT, |now| now != want);
}

impl Plugin for IcecastEndpoint {
    type Config = Config;
    type SystemConfig = trunk_recorder_plugin::NoConfig;

    fn manifest() -> Manifest {
        Manifest {
            name: "Icecast Endpoint".into(),
            subscribe: vec![topic::AUDIO.into(), topic::CALL_START.into(), topic::CALL_END.into(), topic::CALL_CONCLUDED.into()],
            // New settings restart the feeds, not the plugin: a feed filled in from Broadcastify
            // is saved by the plugin itself (settings.save), which only a plugin taking settings
            // as it runs may do.
            live_settings: true,
            // A system can have feeds of its own (for it alone; see start).
            system_overrides: vec!["feeds".into()],
            // The id, version, description, … come from Cargo.toml.
            ..trunk_recorder_plugin::manifest!()
        }
    }

    fn start(host: Host, setup: Setup<Config, Self::SystemConfig>) -> Result<Self, String> {
        let planned = plan(&host, &setup)?;
        let health = Health::new(host.clone());
        let routes = launch(planned, &health)?;
        say_if_no_feeds(&host, &routes);
        let api = api_of(&setup.config);
        let typed: Typed = Arc::new((Mutex::new(typed_of(&setup.config)), Condvar::new()));
        let feeds_now = feeds_now(&setup);
        Ok(IcecastEndpoint {
            routes,
            calls: HashMap::new(),
            playing: HashMap::new(),
            early: HashMap::new(),
            routed: HashMap::new(),
            health,
            host,
            api,
            workers: Vec::new(),
            pending: Pending::default(),
            typed,
            account: Arc::new(Mutex::new(None)),
            looking: Looking::default(),
            feeds_now,
        })
    }

    /// New settings (the plugin takes them as it runs): the feeds start again with them, once
    /// they're all good; a mistake leaves the feeds as they were.
    fn settings(&mut self, setup: Setup<Config, Self::SystemConfig>) -> Result<(), String> {
        self.api = api_of(&setup.config);
        {
            let (m, cv) = &*self.typed;
            *m.lock().unwrap() = typed_of(&setup.config);
            cv.notify_all();
        }
        // Only the Broadcastify login (typed, or cleared) or the list changed: the feeds stream on.
        let now = feeds_now(&setup);
        if now == self.feeds_now {
            return Ok(());
        }
        let planned = plan(&self.host, &setup)?;
        self.feeds_now = now;
        // The calls playing end on the feeds as they were (the next calls go to the new ones).
        for (id, (routes, _)) in self.playing.drain() {
            for r in routes {
                self.routes[r].feed.send(Msg::End { id });
            }
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        for r in &mut self.routes {
            r.feed.close();
        }
        for r in self.routes.drain(..) {
            r.feed.finish(deadline);
        }
        self.health.forget_feeds();
        self.routes = launch(planned, &self.health)?;
        say_if_no_feeds(&self.host, &self.routes);
        Ok(())
    }

    /// Load my Broadcastify feeds, and Use this feed: each on a thread of its own (a login over the
    /// internet, a connection to the feed's server), answered once its settings are saved, or with
    /// why they couldn't be.
    fn action(&mut self, a: Action, reply: Reply) {
        let (api, host, pending, typed, account) = (self.api.clone(), self.host.clone(), self.pending.clone(), self.typed.clone(), self.account.clone());
        let (feeds_now, looking) = (self.feeds_now.0.clone(), self.looking.clone());
        let work: Work = match a.action.as_str() {
            LOAD_FEEDS => {
                // Counted now, on the thread reading messages, so a press on the list after it waits.
                *looking.0.lock().unwrap() += 1;
                Box::new(move || {
                    wait_for_typed(&typed, &typed_in(&a.value));
                    let r = load_feeds(&api, &a.value).map(|(save, done, by_hand, looked)| {
                        *account.lock().unwrap() = Some(looked);
                        (save, done, by_hand)
                    });
                    *looking.0.lock().unwrap() -= 1;
                    looking.1.notify_all();
                    r
                })
            }
            USE_FEED => Box::new(move || {
                let (m, cv) = &*looking;
                drop(cv.wait_timeout_while(m.lock().unwrap(), Duration::from_secs(12), |n| *n > 0).unwrap());
                let mut account = account.lock().unwrap();
                let r = use_feed(&account, &a.value, &feeds_now)?;
                // Another feed can be used next, over the feeds as they'll be.
                if let Some(a) = account.as_mut() {
                    a.settings["feeds"] = r.0["feeds"].clone();
                }
                Ok(r)
            }),
            other => {
                reply.fail(format!("There's no button called {other}"));
                return;
            }
        };
        self.workers.retain(|w| !w.is_finished());
        let w = std::thread::spawn(move || match work() {
            Err(why) => reply.fail(why),
            Ok((save, done, by_hand)) => {
                // Kept before it's saved, so the answer finds it.
                let mut p = pending.lock().unwrap();
                let request = host.save_settings(&save);
                p.insert(request, (reply, done, by_hand));
            }
        });
        self.workers.push(w);
    }

    fn settings_saved(&mut self, request: u64, ok: bool, message: String) {
        let Some((reply, done, by_hand)) = self.pending.lock().unwrap().remove(&request) else { return };
        if ok {
            reply.ok(done);
        } else {
            reply.fail(format!("The settings couldn't be saved ({message}). {by_hand}"));
        }
    }

    fn call_start(&mut self, call: CallInfo) {
        self.release_early(false);
        self.forget_old();
        // A call not recorded here has no audio of its own; another site's copy may,
        // and is routed when its audio comes (as this call).
        if call.recording {
            self.route(call.id, &call.short_name, &talkgroups(&call), Title::of(&call), skip(&call), call.start_time);
        }
        // Audio that came before it, perhaps as another site's copy: the call's.
        let of = |id: u32, e: &Early| id == call.id || e.chunks.first().is_some_and(|c| c.short_name == call.short_name && c.talkgroup == call.talkgroup);
        let mut early: Vec<u32> = self.early.iter().filter(|(id, e)| of(**id, e)).map(|(id, _)| *id).collect();
        early.sort_unstable();
        let id = call.id;
        self.calls.insert(id, call);
        for e in early {
            for chunk in self.early.remove(&e).map(|e| e.chunks).unwrap_or_default() {
                self.play(id, chunk);
            }
        }
    }

    fn audio(&mut self, chunk: AudioChunk) {
        self.release_early(false);
        // A call heard on several sites plays from whichever copy has it: one that wasn't
        // told to start is the call that was (the same system and talkgroup).
        let id = if self.playing.contains_key(&chunk.call_id) || self.calls.contains_key(&chunk.call_id) {
            chunk.call_id
        } else {
            self.calls.values().find(|c| c.short_name == chunk.short_name && c.talkgroup == chunk.talkgroup).map_or(chunk.call_id, |c| c.id)
        };
        if self.playing.contains_key(&id) || self.calls.contains_key(&id) {
            return self.play(id, chunk);
        }
        // Not told of yet: its start says what plays it (patched talkgroups, its title, encrypted).
        let e = self.early.entry(id).or_insert_with(|| Early { since: Instant::now(), chunks: Vec::new(), ms: 0 });
        e.ms += chunk.samples().len() * 1000 / chunk.sample_rate.max(1) as usize;
        e.chunks.push(chunk);
        if e.ms >= EARLY_MOST_MS {
            self.release_early(false);
        }
    }

    fn call_end(&mut self, call: CallInfo) {
        self.release_early(false);
        // (Its start never came.)
        if let Some(e) = self.early.remove(&call.id) {
            self.calls.insert(call.id, call.clone());
            for chunk in e.chunks {
                self.play(call.id, chunk);
            }
        }
        self.calls.remove(&call.id);
        if let Some((routes, _)) = self.playing.remove(&call.id) {
            for r in routes {
                self.routes[r].feed.send(Msg::End { id: call.id });
            }
        }
    }

    /// A recorded call's files are written: what became of it goes to the recorder's counts.
    fn call_concluded(&mut self, c: ConcludedCall) {
        let r = &c.call;
        let id = u32::try_from(r.call_num).ok().filter(|id| self.routed.get(id).is_some_and(|d| d.short_name == r.short_name));
        // Another site's copy of the call: the same system and talkgroup, at about the same time.
        let id = id.or_else(|| {
            self.routed
                .iter()
                .filter(|(_, d)| d.short_name == r.short_name && d.talkgroup == r.talkgroup && (d.start - r.start_time as f64).abs() <= TWIN_WITHIN)
                .min_by(|a, b| (a.1.start - r.start_time as f64).abs().total_cmp(&(b.1.start - r.start_time as f64).abs()))
                .map(|(id, _)| *id)
        });
        // Each call's files come once: a later copy isn't matched to it.
        match id.and_then(|id| self.routed.remove(&id).map(|d| (id, d))) {
            None => self.health.skipped(&c.path, "not heard live (the plugin streams calls as they're recorded)"),
            Some((_, d)) if d.encrypted => self.health.skipped(&c.path, "encrypted"),
            Some((_, d)) if d.feeds == 0 => self.health.skipped(&c.path, "not on any feed"),
            Some((id, _)) => self.health.concluded(id, &c.path),
        }
    }

    fn shutdown(&mut self, grace: Duration) {
        let deadline = Instant::now() + grace;
        self.release_early(true);
        // A press being answered finishes first (its reply goes before the plugin does).
        for w in self.workers.drain(..) {
            while !w.is_finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            if w.is_finished() {
                let _ = w.join();
            }
        }
        for r in &mut self.routes {
            r.feed.close();
        }
        for r in self.routes.drain(..) {
            r.feed.finish(deadline);
        }
    }
}

/// A feed checked and ready to start: where it goes, what it plays, and how.
struct Planned {
    settings: feed::Settings,
    which: String,
    system: String,
    filters: Filters,
    only: Option<String>,
    not: Arc<HashSet<String>>,
    title_format: String,
}

/// Every feed the settings have, checked before any starts (and what's worth saying about each).
fn plan(host: &Host, setup: &Setup<Config, trunk_recorder_plugin::NoConfig>) -> Result<Vec<Planned>, String> {
    // Systems can override the feeds: theirs are for them alone.
    let own: Vec<(&str, &Vec<FeedConfig>)> =
        setup.systems.iter().filter(|s| s.settings.feeds != setup.config.feeds).map(|s| (s.short_name.as_str(), &s.settings.feeds)).collect();
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
        let filters = f.filters().map_err(|e| format!("{which}: {e}"))?;
        if checked.iter().any(|(_, _, t, _, _): &(Option<&str>, &FeedConfig, Target, Filters, String)| {
            t.host == target.host && t.port == target.port && t.mount == target.mount
        }) {
            return Err(format!("{which}: another feed streams to the same mount ({target}); a mount takes one source."));
        }
        checked.push((only, f, target, filters, which));
    }
    let mut planned = Vec::new();
    for (only, f, target, Filters { plays: filter, left, right, old }, which) in checked {
        if !f.system.is_empty() && setup.system_named(&f.system).is_none() {
            host.warn(format!("{which}: there's no system called {}", f.system));
        }
        if old {
            host.warn(format!(
                "{which}: playing the talkgroups saved by an earlier version ({}); choose Talkgroups on the feed to see and change them",
                if filter.is_empty() { "every talkgroup".to_string() } else { filter.describe() }
            ));
        }
        let name = if f.name.trim().is_empty() { target.to_string() } else { format!("{} ({target})", f.name.trim()) };
        let of = match (only, f.system.as_str()) {
            (Some(n), _) | (None, n) if !n.is_empty() => format!(" of {n}"),
            _ => String::new(),
        };
        let tgs = if filter.is_empty() { "every talkgroup".to_string() } else { filter.describe() };
        let mut sides = String::new();
        for (side, s) in [("left", &f.left_talkgroups), ("right", &f.right_talkgroups)] {
            // Checked by filters(): the list as read.
            let s = talkgroup_list(s).unwrap_or_default();
            if !s.is_empty() {
                sides += &format!(", {} on the {side}", s.join(", "));
            }
        }
        let channels = if f.stereo() { "stereo" } else { "mono" };
        let volume = if f.volume == 1.0 { String::new() } else { format!(", volume ×{}", f.volume) };
        host.info(format!("feed {name}: {tgs}{of}{sides}, {} kbps {channels} at {} Hz{volume}", f.bitrate, f.sample_rate));
        let settings = feed::Settings {
            target,
            name,
            max_delay: Duration::from_secs(f.max_delay_seconds as u64),
            update_title: f.update_title,
            idle: (f.idle_seconds > 0).then(|| (Duration::from_secs(f.idle_seconds as u64), f.idle_title.trim().to_string())),
            volume: f.volume as f32,
        };
        let title_format = if f.title_format.trim().is_empty() { TITLE_FORMAT.to_string() } else { f.title_format.trim().to_string() };
        let filters = Filters { plays: filter, left, right, old };
        planned.push(Planned { settings, which, system: f.system.clone(), filters, only: only.map(String::from), not: not.clone(), title_format });
    }
    Ok(planned)
}

/// Start the feeds planned.
fn launch(planned: Vec<Planned>, health: &Health) -> Result<Vec<Route>, String> {
    let mut routes = Vec::new();
    for p in planned {
        let feed = Feed::start(p.settings, health.clone()).map_err(|e| format!("{}: {e}", p.which))?;
        let Filters { plays: filter, left, right, .. } = p.filters;
        routes.push(Route { feed, system: p.system, filter, left, right, only: p.only, not: p.not, title_format: p.title_format });
    }
    Ok(routes)
}

/// No feeds: said in the log and on the plugin's card (its health), with what to do.
fn say_if_no_feeds(host: &Host, routes: &[Route]) {
    if routes.is_empty() {
        let what = "No feeds yet: add one, or load your Broadcastify feeds (Load my Broadcastify feeds) and use one.";
        host.warn(what);
        host.status(State::Warning, what);
    }
}

/// Where a Broadcastify account's feeds come from.
fn api_of(_c: &Config) -> String {
    #[cfg(test)]
    if !_c.broadcastify_api.trim().is_empty() {
        return _c.broadcastify_api.trim().to_string();
    }
    broadcastify::API.to_string()
}

/// Load my Broadcastify feeds (`value`: the settings as edited): log in and list the account's
/// feeds. Ok: the settings to save (the list, and the password cleared), what to say once they're
/// saved, what to say if they can't be, and the account's feeds (kept to use one from the list).
/// Err: what to tell the user.
fn load_feeds(api: &str, value: &Value) -> Result<(Value, String, String, Account), String> {
    let (user, password) = typed_in(value);
    let user = user.trim();
    if user.is_empty() || password.is_empty() {
        return Err("Type your Broadcastify username and password first.".into());
    }
    let feeds = broadcastify::feeds(api, user, &password)?;
    let list: Vec<Listed> = feeds.iter().map(Listed::of).collect();
    let done = match &feeds[..] {
        [f] => format!(
            "Found your feed {} ({}), listed under Your Broadcastify feeds: press Use this feed on it to stream to it. Your Broadcastify password wasn't kept.",
            f.id,
            f.name.trim()
        ),
        _ => format!(
            "Found {} feeds, listed under Your Broadcastify feeds: open the one to stream to and press Use this feed. Your Broadcastify password wasn't kept.",
            feeds.len()
        ),
    };
    let names = feeds.iter().map(|f| format!("{} ({})", f.id, f.name.trim())).collect::<Vec<_>>().join(", ");
    let by_hand = format!(
        "Your feeds: {names}. Add the one to stream to by hand (its server, mount and password are on its Technical tab at broadcastify.com), and clear the Broadcastify password."
    );
    let save = json!({ "broadcastifyFeeds": list, "broadcastifyPassword": null });
    Ok((save, done, by_hand, Account { feeds, settings: value.clone() }))
}

/// Use this feed (`value`: its row on the list): from the account as last loaded, unless the feeds
/// have changed since (`now`: as set).
fn use_feed(account: &Option<Account>, value: &Value, now: &[FeedConfig]) -> Result<(Value, String, String), String> {
    let again = "Press Load my Broadcastify feeds again (with your password), then Use this feed.";
    let Some(a) = account else {
        return Err(format!("The plugin has restarted since your feeds were loaded. {again}"));
    };
    let then: Config = serde_json::from_value(json!({ "feeds": a.settings["feeds"] })).unwrap_or_default();
    if then.feeds != now {
        return Err(format!("The feeds have changed since your Broadcastify feeds were loaded. {again}"));
    }
    let Some(id) = value.get("id").and_then(Value::as_u64).and_then(|n| u32::try_from(n).ok()) else {
        return Err(format!("That row has no feed ID. {again}"));
    };
    let f = broadcastify::pick(&a.feeds, id)?;
    Ok(fill(&a.settings, f))
}

/// The settings (as edited) with `f` filled in, to save; what to say once saved, and what to say if
/// they can't be.
fn fill(settings: &Value, f: &BcfyFeed) -> (Value, String, String) {
    let (all, added) = broadcastify::fill_in(&settings["feeds"], f);
    let at = format!("{}:{}{}", f.host, f.port, f.mount());
    let mut done = if added {
        format!("Added feed {} ({}) at {at}. It plays every system and talkgroup until you choose (System, Talkgroups).", f.id, f.name.trim())
    } else {
        format!("Filled in the feed streaming to {at} (feed {}, {}) with its server, port and password.", f.id, f.name.trim())
    };
    // A network that lets the web through may still stop the stream.
    if let Err(why) = broadcastify::reachable(&f.host, f.port) {
        done = format!("{done} But: {why}");
    }
    let by_hand =
        format!("Add it by hand: server {}, port {}, mount {}; its password is on the feed's Technical tab at broadcastify.com.", f.host, f.port, f.mount());
    (json!({ "feeds": all }), done, by_hand)
}

impl IcecastEndpoint {
    /// A chunk of call `id`'s audio, to the feeds that play it.
    fn play(&mut self, id: u32, chunk: AudioChunk) {
        if !self.playing.contains_key(&id) {
            let (tgs, t, s, start) = match self.calls.get(&id) {
                Some(c) => (talkgroups(c), Title::of(c), skip(c), c.start_time),
                None => {
                    (vec![chunk.talkgroup], Title { alias: String::new(), talkgroup: chunk.talkgroup, system: chunk.short_name.clone() }, false, unix_now())
                }
            };
            self.route(id, &chunk.short_name, &tgs, t, s, start);
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

    /// Audio whose call's start hasn't come in good time (or `all` of it) plays without it.
    fn release_early(&mut self, all: bool) {
        let mut due: Vec<u32> = self.early.iter().filter(|(_, e)| all || e.ms >= EARLY_MOST_MS || e.since.elapsed() >= EARLY_WAIT).map(|(id, _)| *id).collect();
        due.sort_unstable();
        for id in due {
            for chunk in self.early.remove(&id).map(|e| e.chunks).unwrap_or_default() {
                self.play(id, chunk);
            }
        }
    }

    /// Start a call on the feeds that play it, and remember which.
    fn route(&mut self, id: u32, short_name: &str, talkgroups: &[u32], title: Title, skip: bool, start: f64) {
        if self.playing.contains_key(&id) {
            return;
        }
        let routes: Vec<usize> = if skip { Vec::new() } else { (0..self.routes.len()).filter(|&i| self.routes[i].wants(short_name, talkgroups)).collect() };
        if !routes.is_empty() {
            self.health.routed(id, routes.len());
        }
        let talkgroup = talkgroups.first().copied().unwrap_or_default();
        let routed = Routed { short_name: short_name.to_string(), talkgroup, start, feeds: routes.len(), encrypted: skip, at: Instant::now() };
        self.routed.insert(id, routed);
        for &r in &routes {
            let pan = pan(&self.routes[r].left, &self.routes[r].right, talkgroups);
            let title = title.render(&self.routes[r].title_format);
            self.routes[r].feed.send(Msg::Start { id, title, pan });
        }
        self.playing.insert(id, (routes, Instant::now()));
    }

    /// Calls whose end never came (the recorder drops events for a plugin that falls behind),
    /// and calls whose files never came (not recorded after all).
    fn forget_old(&mut self) {
        if self.routed.len() >= MAX_TRACKED {
            self.routed.retain(|_, d| d.at.elapsed() < FORGET_AFTER);
        }
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
        if !(0.1..=10.0).contains(&self.volume) {
            return Err(format!("the volume is {}; make it from 0.1 to 10 (1 is as recorded)", self.volume));
        }
        if !(5..=600).contains(&self.max_delay_seconds) {
            return Err(format!("the longest delay is {} seconds; make it from 5 to 600", self.max_delay_seconds));
        }
        Mp3::new(self.sample_rate, self.bitrate, self.stereo())?;
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
                stereo: self.stereo(),
            },
        })
    }

    /// Talkgroups panned to a side make the feed stereo.
    fn stereo(&self) -> bool {
        self.left_talkgroups.iter().chain(&self.right_talkgroups).any(|s| !s.trim().is_empty())
    }

    /// The Talkgroups menu as it applies: the feed's choice, else (a feed saved by 0.4.0 or
    /// earlier, before the menu) what its old lists come to, and whether they did.
    fn chosen(&self) -> (Talkgroups, bool) {
        if self.menu {
            return (self.talkgroups.clone(), false);
        }
        let (allow, deny) = (self.old_talkgroups.clone(), self.old_exclude_talkgroups.clone());
        // 0.4.0: a feed played its talkgroups and its sides', or (all empty) every talkgroup.
        let mode = if !allow.is_empty() || self.stereo() {
            TalkgroupMode::Only
        } else if !deny.is_empty() {
            TalkgroupMode::AllBut
        } else {
            return (self.talkgroups.clone(), false);
        };
        let old = !allow.is_empty() || !deny.is_empty();
        (Talkgroups { talkgroup_mode: mode, talkgroup_allow: allow, talkgroup_deny: deny }, old)
    }

    /// Which talkgroups the feed plays, and which on each side; errors name an entry that
    /// isn't a talkgroup.
    fn filters(&self) -> Result<Filters, String> {
        let side = |list: &[String], side: &str| TalkgroupFilter::parse(list, &[]).map_err(|e| format!("talkgroups on the {side}: {e}"));
        let (left, right) = (side(&self.left_talkgroups, "left")?, side(&self.right_talkgroups, "right")?);
        let (t, old) = self.chosen();
        let plays = match t.talkgroup_mode {
            // Talkgroups on a side are ones the feed plays.
            TalkgroupMode::Only => {
                let mut allow: Vec<String> = t.talkgroup_allow.iter().chain(&self.left_talkgroups).chain(&self.right_talkgroups).cloned().collect();
                // (Once each: a side's talkgroups are often listed too.)
                let mut seen = HashSet::new();
                allow.retain(|t| seen.insert(t.trim().to_string()));
                let f = TalkgroupFilter::parse(&allow, &t.talkgroup_deny).map_err(|e| format!("talkgroups: {e}"))?;
                if talkgroup_list(&allow).unwrap_or_default().is_empty() {
                    return Err("talkgroups: list the talkgroups for \"Only some\" (or on a side), or choose \"Every talkgroup\"".into());
                }
                f
            }
            _ => t.filter().map_err(|e| format!("talkgroups: {e}"))?,
        };
        Ok(Filters { plays, left, right, old })
    }
}

/// The call's talkgroup, then those patched with it.
fn talkgroups(c: &CallInfo) -> Vec<u32> {
    let mut v = vec![c.talkgroup];
    v.extend(c.patched_talkgroups.iter().filter(|&&t| t != c.talkgroup));
    v
}

/// A feed's title unless it says otherwise: "Fire Dispatch (101)".
const TITLE_FORMAT: &str = "{alias} ({talkgroup})";

/// What the stream's title says while a call plays, before a feed's format.
#[derive(Clone, Debug, PartialEq)]
struct Title {
    alias: String,
    talkgroup: u32,
    system: String,
}

impl Title {
    fn of(c: &CallInfo) -> Title {
        Title { alias: c.talkgroup_tag.trim().to_string(), talkgroup: c.talkgroup, system: c.short_name.clone() }
    }

    /// `format` with `{alias}`, `{talkgroup}` and `{system}` filled in. A talkgroup with no alias
    /// is "Talkgroup 101", whatever the format.
    fn render(&self, format: &str) -> String {
        if self.alias.is_empty() {
            return format!("Talkgroup {}", self.talkgroup);
        }
        let t = format.replace("{alias}", &self.alias).replace("{talkgroup}", &self.talkgroup.to_string()).replace("{system}", &self.system);
        let t = t.trim();
        if t.is_empty() { self.alias.clone() } else { t.to_string() }
    }
}

fn unix_now() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

/// Where a call is heard: as the first of its talkgroups on a side says (one on both is in the center).
fn pan(left: &TalkgroupFilter, right: &TalkgroupFilter, talkgroups: &[u32]) -> Pan {
    let on = |f: &TalkgroupFilter, t| !f.is_empty() && f.passes(t);
    talkgroups
        .iter()
        .find_map(|&t| match (on(left, t), on(right, t)) {
            (true, false) => Some(Pan::Left),
            (false, true) => Some(Pan::Right),
            (true, true) => Some(Pan::Center),
            (false, false) => None,
        })
        .unwrap_or(Pan::Center)
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
    use trunk_recorder_plugin::testing::{MockServer, Request};
    use trunk_recorder_plugin::{EXIT_CONFIG, HostMessage, Level, Outcome, PluginMessage, State, SystemInfo, testing};

    fn call(id: u32, tg: u32) -> CallInfo {
        CallInfo {
            id,
            short_name: "sys1".into(),
            talkgroup: tg,
            talkgroup_tag: format!("TG {tg}"),
            recording: true,
            units: vec![1234],
            start_time: 1_700_000_000.0 + id as f64,
            ..Default::default()
        }
    }

    /// Its files are written, as `call_num` (another site's copy may have another).
    fn concluded(c: &CallInfo, call_num: u32) -> HostMessage {
        let mut k = testing::call(&testing::temp_dir("icecast-endpoint"), &c.short_name, c.talkgroup);
        (k.path, k.call.call_num, k.call.start_time) = (format!("sys1/{call_num}"), call_num as u64, c.start_time as i64);
        HostMessage::CallConcluded(k)
    }

    /// What became of each call: path, and outcome.
    fn results(out: &testing::Output) -> Vec<(String, Outcome)> {
        let mut r: Vec<(String, Outcome)> = out.results().into_iter().map(|(p, o, _, _)| (p, o)).collect();
        r.sort_by(|a, b| a.0.cmp(&b.0));
        r
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

    /// A feed's title: its format with the call's alias, talkgroup and system. A talkgroup with no
    /// alias is "Talkgroup 101".
    #[test]
    fn titles_as_the_feed_says() {
        let t = Title { alias: "Fire Dispatch".into(), talkgroup: 101, system: "county".into() };
        assert_eq!(t.render(TITLE_FORMAT), "Fire Dispatch (101)");
        assert_eq!(t.render("{alias}"), "Fire Dispatch");
        assert_eq!(t.render("{system} - {alias}"), "county - Fire Dispatch");
        assert_eq!(t.render("  "), "Fire Dispatch", "an empty format: the alias");
        let none = Title { alias: String::new(), ..t };
        assert_eq!(none.render("{alias}"), "Talkgroup 101");
        assert_eq!(none.render(TITLE_FORMAT), "Talkgroup 101");
        // Settings: the format, and the title when quiet, as saved; older settings get today's.
        let f: FeedConfig = serde_json::from_value(json!({ "titleFormat": "{alias}", "idleTitle": "Scanning", "idleSeconds": 10 })).unwrap();
        assert_eq!((f.title_format.as_str(), f.idle_title.as_str(), f.idle_seconds), ("{alias}", "Scanning", 10));
        let old: FeedConfig = serde_json::from_value(json!({ "updateTitle": true })).unwrap();
        assert_eq!((old.title_format.as_str(), old.idle_title.as_str(), old.idle_seconds), (TITLE_FORMAT, "", 5));
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
                "volume",
                "system",
                "talkgroupMode",
                "talkgroupAllow",
                "talkgroupDeny",
                "leftTalkgroups",
                "rightTalkgroups",
                "updateTitle",
                "titleFormat",
                "idleTitle",
                "idleSeconds",
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
        assert_eq!(p["volume"]["default"], 1.0);
        assert_eq!((p["volume"]["minimum"].as_f64(), p["volume"]["maximum"].as_f64()), (Some(0.1), Some(10.0)));
        assert_eq!(p["leftTalkgroups"]["default"], json!([]));
        assert_eq!(p["rightTalkgroups"]["type"], "array");
        // The recorder's Talkgroups menu, and its lists only where they apply.
        assert_eq!(p["talkgroupMode"]["enum"], json!(["all", "only", "allBut"]));
        assert_eq!(p["talkgroupMode"]["default"], "all");
        assert_eq!(p["talkgroupAllow"]["x-when"], json!({ "talkgroupMode": "only" }));
        assert_eq!(p["talkgroupDeny"]["x-when"], json!({ "talkgroupMode": ["only", "allBut"] }));
        for list in ["talkgroupAllow", "talkgroupDeny", "leftTalkgroups", "rightTalkgroups"] {
            assert_eq!(p[list]["x-talkgroups"], true, "{list}");
            assert_eq!(p[list]["items"]["type"], "string", "{list}");
        }
        // 0.4.0's lists are read, not shown.
        assert!(p.get("talkgroups").is_none() && p.get("excludeTalkgroups").is_none());
        assert_eq!(f["required"], json!(["server", "mount", "password"]));
        // Load my Broadcastify feeds: the login, and the button, on the whole settings.
        let top = &c["properties"];
        assert_eq!(top["broadcastifyPassword"]["x-secret"], true);
        assert!(top["broadcastifyUsername"].is_object() && top.get("broadcastifyFeedId").is_none());
        assert!(top.get("broadcastifyApi").is_none(), "for testing only: not on the form");
        assert_eq!(c["x-actions"][0]["id"], LOAD_FEEDS);
        assert_eq!(c["x-actions"][0]["label"], "Load my Broadcastify feeds");
        // The account's feeds, listed by the plugin: read-only rows, each titled, with its button.
        let list = &top["broadcastifyFeeds"];
        assert_eq!(list["readOnly"], true);
        let item = &list["items"];
        assert_eq!(item["x-title"], "name");
        assert_eq!(item["x-summary"], json!(["Feed {id}", "{server}:{port}{mount}"]));
        assert_eq!((&item["x-actions"][0]["id"], &item["x-actions"][0]["label"]), (&json!(USE_FEED), &json!("Use this feed")));
    }

    /// Broadcastify's API, as the appliance's: an account's feeds for its username and password.
    fn broadcastify_api(feed_host: &'static str, feed_port: u16) -> MockServer {
        MockServer::start(move |r: &Request| {
            let body = String::from_utf8_lossy(&r.body).to_string();
            if !body.contains("u=me") || !body.contains("p=right") {
                return (200, r#"{"error":true,"errorMsg":"Invalid Username or Password"}"#.into());
            }
            let feed = json!([{ "feedId": "38565", "feedDescr": "Augusta Fire", "mount": "abcd1234", "source_password": "s3cret", "hostname_ext": feed_host, "port_ext": feed_port.to_string() }]);
            (200, feed.to_string())
        })
    }

    fn login(api: &MockServer, password: &str, feeds: Value) -> Value {
        json!({ "feeds": feeds, "broadcastifyUsername": "me", "broadcastifyPassword": password, "broadcastifyApi": api.url() })
    }

    /// Saves of the run, in order.
    fn saves(out: &testing::Output) -> Vec<Value> {
        out.messages
            .iter()
            .filter_map(|m| if let PluginMessage::SettingsSave { settings, .. } = m { Some(Value::Object(settings.clone())) } else { None })
            .collect()
    }

    /// A button's answer.
    fn answer(out: &testing::Output, request: u64) -> (bool, String) {
        out.messages
            .iter()
            .find_map(|m| if let PluginMessage::ActionResult { request: r, ok, message } = m { (*r == request).then(|| (*ok, message.clone())) } else { None })
            .expect("an answer")
    }

    /// Load my Broadcastify feeds: the account's feeds listed (its only one too), the password
    /// cleared; turned down, or nothing typed in, it says so.
    #[test]
    fn loading_an_accounts_feeds() {
        let listening = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listening.local_addr().unwrap().port();
        let api = broadcastify_api("127.0.0.1", port);
        let (save, done, by_hand, account) = load_feeds(api.url(), &login(&api, "right", json!([]))).unwrap();
        assert_eq!(
            save,
            json!({ "broadcastifyFeeds": [{ "id": 38565, "name": "Augusta Fire", "server": "127.0.0.1", "port": port, "mount": "/abcd1234" }], "broadcastifyPassword": null }),
            "the list (no source password), and the password cleared"
        );
        assert!(done.starts_with("Found your feed 38565 (Augusta Fire)") && done.contains("Use this feed") && done.contains("password wasn't kept"), "{done}");
        assert!(by_hand.contains("38565 (Augusta Fire)") && !by_hand.contains("s3cret"), "{by_hand}");
        assert_eq!(account.feeds.len(), 1);
        assert!(!format!("{account:?}").contains("s3cret"), "its passwords not shown");
        // What's saved is good settings (null removes a setting, as the recorder saves it).
        let mut kept = save.as_object().unwrap().clone();
        kept.retain(|_, v| !v.is_null());
        let config: Config = serde_json::from_value(Value::Object(kept)).unwrap();
        assert_eq!(config.broadcastify_feeds[0].id, 38565);
        assert!(config.broadcastify_password.is_empty());
        // Turned down, or nothing typed in.
        assert!(load_feeds(api.url(), &login(&api, "wrong", json!([]))).unwrap_err().contains("didn't accept that username and password"));
        assert!(load_feeds(api.url(), &json!({ "broadcastifyUsername": "me" })).unwrap_err().contains("Type your Broadcastify username and password"));
    }

    /// Use this feed: added (or the one on its mount filled in), the password from the account;
    /// a feed server that can't be reached is said.
    #[test]
    fn using_a_listed_feed() {
        let listening = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listening.local_addr().unwrap().port();
        let api = broadcastify_api("127.0.0.1", port);
        let (_, _, _, account) = load_feeds(api.url(), &login(&api, "right", json!([]))).unwrap();
        let account = Some(account);
        let (save, done, by_hand) = use_feed(&account, &json!({ "id": 38565 }), &[]).unwrap();
        assert_eq!(save, json!({ "feeds": [{ "name": "Augusta Fire", "server": "127.0.0.1", "port": port, "mount": "/abcd1234", "password": "s3cret" }] }));
        assert!(done.starts_with(&format!("Added feed 38565 (Augusta Fire) at 127.0.0.1:{port}/abcd1234.")) && !done.contains("But:"), "{done}");
        assert!(by_hand.contains("mount /abcd1234") && !by_hand.contains("s3cret"), "{by_hand}");
        let config: Config = serde_json::from_value(save).unwrap();
        assert!(config.feeds[0].target().is_ok());
        // Its server not taking connections: filled in, and said why it won't stream.
        drop(listening);
        let (_, done, _) = use_feed(&account, &json!({ "id": 38565 }), &[]).unwrap();
        assert!(done.contains("But: This computer couldn't connect to 127.0.0.1") && done.contains("firewall"), "{done}");
    }

    /// The buttons, pressed while recording: Load my Broadcastify feeds saves the list (and
    /// clears the password), Use this feed saves the feed; a wrong login is answered, nothing saved.
    #[test]
    fn the_buttons() {
        let listening = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = broadcastify_api("127.0.0.1", listening.local_addr().unwrap().port());
        let press = |password: &str| {
            let typed = login(&api, password, json!([]));
            let hello = HostMessage::Hello(testing::hello(&testing::temp_dir("icecast-endpoint"), typed.clone()));
            let load = HostMessage::Action(Action { request: 7, action: LOAD_FEEDS.into(), value: typed });
            let pick = HostMessage::Action(Action { request: 8, action: USE_FEED.into(), value: json!({ "id": 38565, "name": "Augusta Fire" }) });
            run(vec![hello, load, pick])
        };
        let out = press("right");
        let saved = saves(&out);
        assert_eq!(saved.len(), 2, "{saved:?}");
        assert_eq!((saved[0]["broadcastifyFeeds"][0]["id"].clone(), saved[0]["broadcastifyPassword"].clone()), (json!(38565), Value::Null));
        assert_eq!(saved[1]["feeds"][0]["mount"], "/abcd1234");
        let out = press("wrong");
        let (ok, message) = answer(&out, 7);
        assert!(!ok && message.contains("didn't accept"), "{message}");
        let (ok, message) = answer(&out, 8);
        assert!(!ok && message.contains("Press Load my Broadcastify feeds again"), "{message}");
        assert!(saves(&out).is_empty());
    }

    /// "Only some" with the sides' talkgroups listed too (as on a feed set up before sides): each
    /// once in what it plays, as the log says it.
    #[test]
    fn only_some_with_sides_lists_each_once() {
        let f: FeedConfig = serde_json::from_value(json!({
            "talkgroupMode": "only", "talkgroupAllow": ["201", "401"], "leftTalkgroups": ["201"], "rightTalkgroups": ["401", "402"]
        }))
        .unwrap();
        assert_eq!(f.filters().unwrap().plays.describe(), "only 201, 401, 402");
    }

    /// An account with two feeds, the second on its own Broadcastify server.
    fn two_feeds_api(feed_port: u16) -> MockServer {
        MockServer::start(move |_: &Request| {
            let feeds = json!([
                { "feedId": "38565", "feedDescr": "Augusta Fire", "mount": "abcd1234", "source_password": "s3cret", "hostname_ext": "127.0.0.1", "port_ext": feed_port.to_string() },
                { "feedId": 40001, "feedDescr": "Augusta Police", "mount": "efgh5678", "source_password": "other", "hostname_ext": "127.0.0.1", "port_ext": feed_port }
            ]);
            (200, feeds.to_string())
        })
    }

    /// An account with several feeds: each listed; then one used, and another after it (over the
    /// feeds as the first left them); the feeds changed meanwhile, or the plugin restarted: load again.
    #[test]
    fn several_feeds_to_choose_from() {
        let listening = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listening.local_addr().unwrap().port();
        let api = two_feeds_api(port);
        let mine = json!([{ "name": "Mine", "server": "x.example", "mount": "/x", "password": "p", "talkgroups": ["101"] }]);
        let (save, done, _, account) = load_feeds(api.url(), &login(&api, "right", mine.clone())).unwrap();
        assert_eq!(
            save["broadcastifyFeeds"],
            json!([
                { "id": 38565, "name": "Augusta Fire", "server": "127.0.0.1", "port": port, "mount": "/abcd1234" },
                { "id": 40001, "name": "Augusta Police", "server": "127.0.0.1", "port": port, "mount": "/efgh5678" }
            ])
        );
        assert!(done.starts_with("Found 2 feeds") && done.contains("Use this feed"), "{done}");
        let mut account = Some(account);
        let saved: Config = serde_json::from_value(json!({ "feeds": mine })).unwrap();
        let (save, done, _) = use_feed(&account, &json!({ "id": 40001 }), &saved.feeds).unwrap();
        assert_eq!(save["feeds"][0], mine[0], "the others exactly as they were (old keys too)");
        assert_eq!((&save["feeds"][1]["mount"], &save["feeds"][1]["password"]), (&json!("/efgh5678"), &json!("other")));
        assert!(done.starts_with("Added feed 40001 (Augusta Police)"), "{done}");
        // (As the plugin does once one's used.)
        account.as_mut().unwrap().settings["feeds"] = save["feeds"].clone();
        let now: Config = serde_json::from_value(json!({ "feeds": save["feeds"] })).unwrap();
        let (save, _, _) = use_feed(&account, &json!({ "id": 38565 }), &now.feeds).unwrap();
        assert_eq!(save["feeds"].as_array().map(Vec::len), Some(3), "a second feed added");
        // Changed since, restarted, a row without its ID, a feed not on the account.
        assert!(use_feed(&account, &json!({ "id": 40001 }), &[]).unwrap_err().contains("changed since"));
        assert!(use_feed(&None, &json!({ "id": 40001 }), &[]).unwrap_err().contains("Press Load my Broadcastify feeds again"));
        assert!(use_feed(&account, &json!({}), &now.feeds).unwrap_err().contains("no feed ID"));
        assert!(use_feed(&account, &json!({ "id": 7 }), &now.feeds).unwrap_err().contains("no feed 7"));
    }

    /// Use this feed pressed while the feeds are still loading waits for them; then two used in
    /// turn, the second over what the first saved.
    #[test]
    fn use_waits_for_the_list() {
        let listening = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = two_feeds_api(listening.local_addr().unwrap().port());
        let typed = login(&api, "right", json!([]));
        let hello = HostMessage::Hello(testing::hello(&testing::temp_dir("icecast-endpoint"), typed.clone()));
        let load = HostMessage::Action(Action { request: 7, action: LOAD_FEEDS.into(), value: typed });
        let one = HostMessage::Action(Action { request: 8, action: USE_FEED.into(), value: json!({ "id": 40001 }) });
        let out = run(vec![hello, load, one]);
        let saved = saves(&out);
        assert_eq!(saved.len(), 2, "{saved:?}");
        assert_eq!(saved[0]["broadcastifyFeeds"].as_array().map(Vec::len), Some(2));
        assert_eq!(saved[1]["feeds"][0]["mount"], "/efgh5678");
    }

    /// A press waits for what was typed just before it to be saved (the form saves 300 ms after the
    /// last change): it goes on as soon as it is, else after a few seconds.
    #[test]
    fn a_press_waits_for_what_was_typed() {
        let typed: Typed = Arc::new((Mutex::new((String::new(), String::new())), Condvar::new()));
        let want = ("me".to_string(), "pw".to_string());
        let t = typed.clone();
        let w = want.clone();
        let saver = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            *t.0.lock().unwrap() = w;
            t.1.notify_all();
        });
        let at = Instant::now();
        wait_for_typed(&typed, &want);
        assert!(at.elapsed() >= Duration::from_millis(100) && at.elapsed() < TYPED_WAIT, "{:?}", at.elapsed());
        saver.join().unwrap();
        // Already saved: at once.
        let at = Instant::now();
        wait_for_typed(&typed, &want);
        assert!(at.elapsed() < Duration::from_millis(50));
        // Never saved: it gives up and goes on.
        let at = Instant::now();
        wait_for_typed(&typed, &("other".into(), String::new()));
        assert!(at.elapsed() >= TYPED_WAIT);
        assert_eq!(typed_in(&json!({ "broadcastifyUsername": "me", "broadcastifyPassword": "pw" })), want);
    }

    /// Settings that leave the feeds as they were (the Broadcastify login typed, the list saved)
    /// don't restart them: the stream goes on.
    #[test]
    fn new_settings_without_new_feeds_leave_them_streaming() {
        let server = fake::Server::ok();
        let settings = |extra: Value| {
            let mut c = json!({ "feeds": [feed(server.port, json!({}))] });
            c.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            HostMessage::Settings(testing::hello(&testing::temp_dir("icecast-endpoint"), c))
        };
        let msgs = vec![
            HostMessage::Hello(testing::hello(&testing::temp_dir("icecast-endpoint"), json!({ "feeds": [feed(server.port, json!({}))] }))),
            settings(json!({ "broadcastifyUsername": "me" })),
            settings(json!({ "broadcastifyUsername": "me", "broadcastifyPassword": "pw" })),
            settings(json!({ "broadcastifyFeeds": [{ "id": 1, "name": "A", "server": "x", "port": 80, "mount": "/a" }] })),
        ];
        let out = run(msgs);
        let started = out.logs().iter().filter(|l| l.1.starts_with("feed ")).count();
        assert_eq!(started, 1, "started once: {:?}", out.logs());
        assert_eq!(server.wait(|c| !c.is_empty()).len(), 1, "one connection");
    }

    /// New settings restart the feeds, not the plugin: a feed added streams, one taken away stops.
    /// Settings with a mistake leave the feeds as they were.
    #[test]
    fn new_settings_restart_the_feeds() {
        assert!(trunk_recorder_plugin::describe::<IcecastEndpoint>().live_settings);
        let (one, two) = (fake::Server::ok(), fake::Server::ok());
        let hello = |feeds: Value| testing::hello(&testing::temp_dir("icecast-endpoint"), json!({ "feeds": feeds }));
        let mut msgs = vec![HostMessage::Hello(hello(json!([feed(one.port, json!({}))])))];
        msgs.push(HostMessage::Settings(hello(json!([feed(two.port, json!({ "mount": "/other" }))]))));
        msgs.push(HostMessage::Settings(hello(json!([feed(two.port, json!({ "mount": " " }))]))));
        let out = run(msgs);
        assert!(out.ready());
        assert!(two.wait(|c| !c.is_empty()).iter().any(|c| c.head.contains("/other")), "the new feed streams");
        assert!(one.wait(|c| c.first().is_some_and(|c| c.closed))[0].closed, "the old one stopped");
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter_map(|m| if let PluginMessage::Status { state: State::Error, message } = m { Some(message.clone()) } else { None })
            .collect();
        assert!(errors.iter().any(|e| e.contains("new settings not taken") && e.contains("mount")), "{errors:?}");
    }

    #[test]
    fn the_manifest() {
        let m = trunk_recorder_plugin::describe::<IcecastEndpoint>();
        assert_eq!(m.subscribe, ["audio", "call.start", "call.end", "call.concluded"]);
        assert!(m.system_config.is_none());
        // A system can have feeds of its own, and nothing else of its own.
        assert_eq!(m.system_overrides, ["feeds"]);
        assert!(m.site_overrides.is_empty());
    }

    /// With no feeds yet, the plugin starts (so a feed can be filled in from Broadcastify) and says
    /// what to do.
    #[test]
    fn no_feeds_yet() {
        let out = run(vec![hello(json!([]))]);
        assert!(out.ready(), "{:?}", out.messages);
        assert!(out.logs().iter().any(|l| l.1.contains("No feeds yet") && l.1.contains("Load my Broadcastify feeds")), "{:?}", out.logs());
        // And on the plugin's card.
        let (state, message) = out.status().expect("a status");
        assert_eq!(state, State::Warning);
        assert!(message.contains("No feeds yet"), "{message}");
    }

    #[test]
    fn bad_settings_say_what_to_fix() {
        let cases = [
            (json!([feed(8000, json!({ "server": "" }))]), "server"),
            (json!([feed(8000, json!({ "server": "https://x.example" }))]), "https"),
            (json!([feed(8000, json!({ "server": "x.example/feed" }))]), "host name"),
            (json!([feed(8000, json!({ "mount": " " }))]), "mount"),
            (json!([feed(8000, json!({ "password": "" }))]), "password"),
            (json!([feed(0, json!({}))]), "port"),
            (json!([feed(8000, json!({ "bitrate": 17 }))]), "17 kbps"),
            (json!([feed(8000, json!({ "sampleRate": 11000 }))]), "11000"),
            (json!([feed(8000, json!({ "maxDelaySeconds": 1 }))]), "delay"),
            (json!([feed(8000, json!({ "volume": 0 }))]), "volume"),
            (json!([feed(8000, json!({ "volume": 11 }))]), "volume"),
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

    /// Each feed's title says what its Title format does: as before by default, the alias alone,
    /// or with the system. (The plugin is driven directly, so the call plays while it runs.)
    #[test]
    fn a_feeds_title_follows_its_format() {
        for (format, sent) in [(json!(null), "TG%20101%20%28101%29"), (json!("{alias}"), "TG%20101"), (json!("{system}: {alias}"), "sys1%3A%20TG%20101")] {
            let server = fake::Server::ok();
            let mut f = feed(server.port, json!({ "updateTitle": true, "idleSeconds": 0 }));
            if !format.is_null() {
                f["titleFormat"] = format.clone();
            }
            let dir = testing::temp_dir("icecast-endpoint");
            let config: Config = serde_json::from_value(json!({ "feeds": [f] })).unwrap();
            let setup =
                Setup { config, systems: Vec::new(), host: Default::default(), capture_dir: dir.clone(), data_dir: dir, audio_formats: vec!["wav".into()] };
            let (host, _) = testing::capture();
            let mut p = IcecastEndpoint::start(host, setup).unwrap();
            p.call_start(call(1, 101));
            let chunk = AudioChunk { short_name: "sys1".into(), ..AudioChunk::new(1, 0, 101, 8000, &[1000; 8000]) };
            p.audio(chunk);
            let c = server.wait(|c| c.iter().any(|c| c.head.contains("&song=")));
            p.shutdown(Duration::from_secs(2));
            let song = c.iter().find_map(|c| c.head.split("&song=").nth(1)).map(|t| t.split(' ').next().unwrap_or_default().to_string());
            assert_eq!(song.as_deref(), Some(sent), "{format}");
        }
    }

    #[test]
    fn talkgroups_on_a_side_make_the_feed_stereo() {
        let server = fake::Server::ok();
        let out = run(vec![hello(json!([feed(server.port, json!({ "bitrate": 32, "leftTalkgroups": ["101"] }))]))]);
        assert!(out.ready(), "{:?}", out.messages);
        let head = &server.wait(|c| !c.is_empty())[0].head;
        assert!(head.contains("ice-audio-info: channels=2;samplerate=22050;bitrate=32\r\n"), "{head}");
        assert!(out.logs().iter().any(|(_, l)| l.contains("101 on the left, 32 kbps stereo")), "{:?}", out.logs());
    }

    #[test]
    fn the_volume_is_logged_when_changed() {
        let server = fake::Server::ok();
        let out = run(vec![hello(json!([feed(server.port, json!({ "volume": 3.0 }))]))]);
        assert!(out.ready(), "{:?}", out.messages);
        assert!(out.logs().iter().any(|(_, l)| l.ends_with("22050 Hz, volume ×3")), "{:?}", out.logs());
        let out = run(vec![hello(json!([feed(server.port, json!({}))]))]);
        assert!(out.logs().iter().any(|(_, l)| l.ends_with("22050 Hz")), "{:?}", out.logs());
    }

    #[test]
    fn where_calls_are_heard() {
        let f = |v: &[&str]| TalkgroupFilter::new(&v.iter().map(|s| s.to_string()).collect::<Vec<_>>(), &[]);
        let (left, right) = (f(&["1*"]), f(&["2*", "150"]));
        assert_eq!(pan(&left, &right, &[101]), Pan::Left);
        assert_eq!(pan(&left, &right, &[202]), Pan::Right);
        assert_eq!(pan(&left, &right, &[303]), Pan::Center);
        // On both sides: the center.
        assert_eq!(pan(&left, &right, &[150]), Pan::Center);
        // A patched call: as its first talkgroup on a side.
        assert_eq!(pan(&left, &right, &[303, 202, 101]), Pan::Right);
        // Mono: no sides.
        assert_eq!(pan(&f(&[]), &f(&[]), &[101]), Pan::Center);
    }

    #[test]
    fn talkgroups_on_a_side_are_played() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["300"], "leftTalkgroups": ["101"], "rightTalkgroups": ["202"] }))]))];
        for (id, tg) in [(1, 101), (2, 202), (3, 300), (4, 404)] {
            m.extend(whole(call(id, tg)));
        }
        // 404 isn't on the feed.
        assert_eq!(played(&run(m)), 3);
    }

    #[test]
    fn sides_alone_say_what_plays() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "leftTalkgroups": ["101"], "excludeTalkgroups": ["202"], "rightTalkgroups": ["202"] }))]))];
        for (id, tg) in [(1, 101), (2, 202), (3, 300)] {
            m.extend(whole(call(id, tg)));
        }
        // 300 isn't on a side (it would need Talkgroups), and 202 is left out.
        assert_eq!(played(&run(m)), 1);
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

    /// The talkgroups that play on a feed of these settings, of 101, 102, 202, 1000, 1050 and 2000.
    fn playing(settings: Value) -> Vec<u32> {
        let tgs = [101, 102, 202, 1000, 1050, 2000];
        let server = fake::Server::ok();
        let mut on = Vec::new();
        for (id, tg) in (1..).zip(tgs) {
            let mut m = vec![hello(json!([feed(server.port, settings.clone())]))];
            m.extend(whole(call(id, tg)));
            let out = run(m);
            assert!(out.ready(), "{settings}: {:?}", out.status());
            if played(&out) == 1 {
                on.push(tg);
            }
        }
        on
    }

    #[test]
    fn talkgroups_chosen_in_the_menu() {
        assert_eq!(playing(json!({ "talkgroupMode": "all" })), [101, 102, 202, 1000, 1050, 2000]);
        assert_eq!(playing(json!({})), [101, 102, 202, 1000, 1050, 2000], "no choice: every talkgroup");
        // A range typed with spaces, and one of it left out.
        let only = json!({ "talkgroupMode": "only", "talkgroupAllow": ["1000 - 1099", 101], "talkgroupDeny": [1050] });
        assert_eq!(playing(only), [101, 1000]);
        // The allow list is kept, hidden, but doesn't apply to "All but some".
        assert_eq!(playing(json!({ "talkgroupMode": "allBut", "talkgroupAllow": ["1*"], "talkgroupDeny": ["10??", "202"] })), [101, 102, 2000]);
        // Talkgroups on a side play with "Only some"; with "Every talkgroup", they're only placed.
        assert_eq!(playing(json!({ "talkgroupMode": "only", "talkgroupAllow": [101], "leftTalkgroups": ["2000"] })), [101, 2000]);
        assert_eq!(playing(json!({ "talkgroupMode": "only", "rightTalkgroups": ["1000-1099"] })), [1000, 1050]);
        assert_eq!(playing(json!({ "talkgroupMode": "all", "leftTalkgroups": [101] })), [101, 102, 202, 1000, 1050, 2000]);
    }

    #[test]
    fn the_menu_is_logged_in_its_words() {
        let server = fake::Server::ok();
        let f =
            feed(server.port, json!({ "talkgroupMode": "only", "talkgroupAllow": ["1000 - 1099"], "talkgroupDeny": [1050], "leftTalkgroups": ["101 102"] }));
        let out = run(vec![hello(json!([f]))]);
        assert!(out.logs().iter().any(|(_, l)| l.contains("only 1000-1099, 101, 102; except 1050, 101, 102 on the left")), "{:?}", out.logs());
        assert!(!out.logs().iter().any(|(_, l)| l.contains("earlier version")), "{:?}", out.logs());
    }

    #[test]
    fn talkgroups_saved_by_0_4_still_apply() {
        assert_eq!(playing(json!({ "talkgroups": ["1*"], "excludeTalkgroups": [102] })), [101, 1000, 1050]);
        assert_eq!(playing(json!({ "excludeTalkgroups": ["1*"] })), [202, 2000], "Leave out alone: all but those");
        assert_eq!(playing(json!({ "leftTalkgroups": ["101"], "rightTalkgroups": ["202"] })), [101, 202], "sides alone: just theirs");
        // A choice in the menu wins over lists left from before.
        assert_eq!(playing(json!({ "talkgroupMode": "all", "talkgroups": ["101"] })), [101, 102, 202, 1000, 1050, 2000]);
        assert_eq!(playing(json!({ "talkgroupMode": "only", "talkgroupAllow": ["202"], "talkgroups": ["101"] })), [202]);
        // And the log says to choose the menu.
        let server = fake::Server::ok();
        let out = run(vec![hello(json!([feed(server.port, json!({ "talkgroups": ["101"] }))]))]);
        let said = out.logs().into_iter().find(|(_, l)| l.contains("earlier version")).expect("a warning");
        assert_eq!(said.0, Level::Warn);
        assert!(said.1.starts_with("Feed County: playing the talkgroups saved by an earlier version (only 101); choose Talkgroups"), "{said:?}");
    }

    #[test]
    fn a_bad_talkgroup_stops_the_plugin() {
        let cases = [
            (json!({ "talkgroupMode": "only", "talkgroupAllow": ["10OO"] }), "Feed County: talkgroups: \"10OO\" isn't a talkgroup"),
            (json!({ "talkgroupMode": "allBut", "talkgroupDeny": ["5a*"] }), "Feed County: talkgroups: \"5a*\" isn't a talkgroup"),
            (json!({ "leftTalkgroups": ["x"] }), "Feed County: talkgroups on the left: \"x\" isn't a talkgroup"),
            (json!({ "rightTalkgroups": ["10-"] }), "Feed County: talkgroups on the right: \"10-\" isn't a talkgroup"),
            (json!({ "talkgroups": ["abc"] }), "Feed County: talkgroups: \"abc\" isn't a talkgroup"),
            (json!({ "talkgroupMode": "only" }), "Feed County: talkgroups: list the talkgroups for \"Only some\""),
        ];
        for (settings, says) in cases {
            let out = run(vec![hello(json!([feed(8000, settings.clone())]))]);
            assert_eq!(out.exit_code, EXIT_CONFIG, "{settings}");
            let (_, msg) = out.status().unwrap();
            assert!(msg.starts_with(says), "{settings}: {msg}");
        }
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

    /// The transmission after another on a channel the recorder kept (conversation mode off, Trunk
    /// Recorder Pro v0.5.1): its first audio comes just before its start.
    fn audio_first(c: CallInfo) -> Vec<HostMessage> {
        let mut m = audio(c.id, c.talkgroup);
        let rest = m.split_off(2);
        m.push(HostMessage::CallStart(c.clone()));
        m.extend(rest);
        m.push(HostMessage::CallEnd(c));
        m
    }

    /// It plays on the feeds its start says, not the ones its audio alone would.
    #[test]
    fn audio_before_its_start_waits_for_it() {
        let server = fake::Server::ok();
        let (mut a, mut b) = (call(1, 101), call(2, 101));
        for c in [&mut a, &mut b] {
            c.patched_talkgroups = vec![101, 202];
        }
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["202"] }))]))];
        m.extend(whole(a));
        m.extend(audio_first(b.clone()));
        m.push(concluded(&b, 2));
        let out = run(m);
        assert_eq!(played(&out), 2);
        assert_eq!(results(&out), [("sys1/2".into(), Outcome::Ok)]);
    }

    /// An encrypted call's is left out, and counted so.
    #[test]
    fn audio_before_an_encrypted_calls_start_is_left_out() {
        let server = fake::Server::ok();
        let mut c = call(1, 101);
        c.encrypted = true;
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(audio_first(c.clone()));
        m.push(concluded(&c, 1));
        let out = run(m);
        assert_eq!(played(&out), 0);
        assert_eq!(results(&out), [("sys1/1".into(), Outcome::Skipped)]);
        assert!(out.results()[0].2.contains("encrypted"), "{:?}", out.results());
    }

    /// Audio of another site's copy, before the call's start: the call's.
    #[test]
    fn a_copys_audio_before_its_calls_start_is_the_calls() {
        let server = fake::Server::ok();
        let mut first = call(1, 101);
        first.recording = false;
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(audio(9, 101));
        m.push(HostMessage::CallStart(first.clone()));
        m.push(HostMessage::CallEnd(first));
        assert_eq!(played(&run(m)), 1);
    }

    /// The plugin itself, with one feed, driven directly (so its calls play while it runs).
    fn plugin(server: &fake::Server, extra: Value) -> IcecastEndpoint {
        let dir = testing::temp_dir("icecast-endpoint");
        let config: Config = serde_json::from_value(json!({ "feeds": [feed(server.port, extra)] })).unwrap();
        let setup = Setup { config, systems: Vec::new(), host: Default::default(), capture_dir: dir.clone(), data_dir: dir, audio_formats: vec!["wav".into()] };
        IcecastEndpoint::start(testing::capture().0, setup).unwrap()
    }

    fn chunk(id: u32, tg: u32, ms: usize) -> AudioChunk {
        AudioChunk { short_name: "sys1".into(), ..AudioChunk::new(id, 0, tg, 8000, &vec![1000; ms * 8]) }
    }

    /// The first title sent, as the server got it.
    fn first_title(server: &fake::Server) -> Option<String> {
        let c = server.wait(|c| c.iter().any(|c| c.head.contains("&song=")));
        c.iter().find_map(|c| c.head.split("&song=").nth(1)).map(|t| t.split(' ').next().unwrap_or_default().to_string())
    }

    /// Its title is the one its start gives.
    #[test]
    fn audio_before_its_start_has_its_title() {
        let server = fake::Server::ok();
        let mut p = plugin(&server, json!({ "updateTitle": true }));
        p.audio(chunk(1, 101, 180));
        p.call_start(call(1, 101));
        p.audio(chunk(1, 101, 800));
        let title = first_title(&server);
        p.shutdown(Duration::from_secs(2));
        assert_eq!(title.as_deref(), Some("TG%20101%20%28101%29"));
    }

    /// No start comes (the recorder dropped it): the call plays anyway, by its talkgroup.
    #[test]
    fn audio_whose_start_never_comes_plays() {
        let server = fake::Server::ok();
        let mut p = plugin(&server, json!({ "updateTitle": true }));
        // Enough of it at once, or (the next test) a while with no start.
        for _ in 0..20 {
            p.audio(chunk(1, 101, 200));
        }
        let title = first_title(&server);
        p.shutdown(Duration::from_secs(2));
        assert_eq!(title.as_deref(), Some("Talkgroup%20101"));
    }

    #[test]
    fn audio_whose_start_is_late_plays() {
        let server = fake::Server::ok();
        let mut p = plugin(&server, json!({ "updateTitle": true }));
        p.audio(chunk(1, 101, 200));
        std::thread::sleep(Duration::from_millis(600));
        // Anything the recorder sends is a chance to see it's waited long enough.
        p.call_end(call(5, 505));
        let title = first_title(&server);
        p.shutdown(Duration::from_secs(2));
        assert_eq!(title.as_deref(), Some("Talkgroup%20101"));
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
    fn each_call_is_counted_sent_or_skipped() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({ "talkgroups": ["101"] }))]))];
        let (sent, other) = (call(1, 101), call(2, 202));
        let mut encrypted = call(3, 101);
        encrypted.encrypted = true;
        for c in [&sent, &other, &encrypted] {
            m.extend(whole(c.clone()));
            m.push(concluded(c, c.id));
        }
        // A call the plugin never heard live (it started after the call did).
        m.push(concluded(&call(4, 101), 4));
        let out = run(m);
        assert_eq!(
            results(&out),
            [("sys1/1".into(), Outcome::Ok), ("sys1/2".into(), Outcome::Skipped), ("sys1/3".into(), Outcome::Skipped), ("sys1/4".into(), Outcome::Skipped)]
        );
        let mut why: Vec<(String, String)> = out.results().into_iter().map(|r| (r.0, r.2)).collect();
        why.sort();
        let why: Vec<&str> = why.iter().map(|w| w.1.as_str()).collect();
        assert_eq!(why, ["", "not on any feed", "encrypted", "not heard live (the plugin streams calls as they're recorded)"]);
    }

    #[test]
    fn a_call_waiting_to_play_is_counted_once_it_has() {
        let server = fake::Server::ok();
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        let (first, second) = (call(1, 101), call(2, 202));
        m.push(HostMessage::CallStart(first.clone()));
        m.push(HostMessage::CallStart(second.clone()));
        m.extend(audio(1, 101));
        m.extend(audio(2, 202));
        m.push(HostMessage::CallEnd(first.clone()));
        m.push(HostMessage::CallEnd(second.clone()));
        // Both are written long before the second plays; it's counted at shutdown, as it plays.
        m.push(concluded(&first, 1));
        m.push(concluded(&second, 2));
        assert_eq!(results(&run(m)), [("sys1/1".into(), Outcome::Ok), ("sys1/2".into(), Outcome::Ok)]);
    }

    #[test]
    fn another_sites_copy_is_counted_as_the_call() {
        let server = fake::Server::ok();
        let c = call(1, 101);
        let mut m = vec![hello(json!([feed(server.port, json!({}))]))];
        m.extend(whole(c.clone()));
        // Concluded as another site's copy, with another number.
        m.push(concluded(&c, 77));
        assert_eq!(results(&run(m)), [("sys1/77".into(), Outcome::Ok)]);
    }

    #[test]
    fn a_call_on_two_feeds_is_counted_once() {
        let (a, b) = (fake::Server::ok(), fake::Server::ok());
        let c = call(1, 101);
        let mut m = vec![hello(json!([feed(a.port, json!({})), feed(b.port, json!({ "name": "Other" }))]))];
        m.extend(whole(c.clone()));
        m.push(concluded(&c, 1));
        assert_eq!(results(&run(m)), [("sys1/1".into(), Outcome::Ok)]);
    }

    #[test]
    fn calls_dropped_or_not_streamed_failed() {
        // Dropped: more waiting than the longest delay (5 s) allows.
        let server = fake::Server::ok();
        let (first, second) = (call(1, 101), call(2, 202));
        let mut m = vec![hello(json!([feed(server.port, json!({ "maxDelaySeconds": 5 }))]))];
        m.push(HostMessage::CallStart(first.clone()));
        m.push(HostMessage::CallStart(second.clone()));
        m.extend(audio(1, 101));
        // 6 s of the second, waiting behind the first.
        m.extend((0..300).map(|_| HostMessage::Audio(AudioChunk { short_name: "sys1".into(), ..AudioChunk::new(2, 0, 202, 8000, &[1000; 160]) })));
        for c in [&first, &second] {
            m.push(HostMessage::CallEnd(c.clone()));
            m.push(concluded(c, c.id));
        }
        let out = run(m);
        assert_eq!(results(&out), [("sys1/1".into(), Outcome::Ok), ("sys1/2".into(), Outcome::Failed)]);
        assert!(out.results().iter().any(|r| r.2.contains("dropped, with more than 5 s of calls waiting")), "{:?}", out.results());

        // Not streamed: the server is down.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let c = call(1, 101);
        let mut m = vec![hello(json!([feed(port, json!({}))]))];
        m.extend(whole(c.clone()));
        m.push(concluded(&c, 1));
        let out = run(m);
        assert_eq!(results(&out), [("sys1/1".into(), Outcome::Failed)]);
        assert!(out.results()[0].2.contains("not streamed, no connection to the server"), "{:?}", out.results());
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
