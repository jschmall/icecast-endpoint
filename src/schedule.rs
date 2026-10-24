//! What a feed plays when: one call at a time, in the order they started,
//! with the others waiting behind it, and silence when there's nothing to
//! play. It has no clock of its own: the feed's thread says what time it is.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::resample::Resampler;

/// A call that has had no audio for this long is over, `call.end` or not.
pub const QUIET_TIMEOUT: Duration = Duration::from_secs(3);
/// Silence between one call and the next.
pub const GAP: Duration = Duration::from_millis(500);
/// A call waits for this much of its audio before it plays (or this long after
/// its first audio), so audio that comes a little late doesn't leave a gap.
pub const PREFILL: Duration = Duration::from_millis(150);
/// Calls waiting at most (beyond this, the oldest waiting is dropped).
const MAX_CALLS: usize = 256;

/// Calls remembered as finished, so late audio doesn't start them again.
const FINISHED: usize = 64;

struct Call {
    id: u32,
    title: String,
    /// Its audio not yet played, at the stream's rate.
    samples: VecDeque<i16>,
    resampler: Option<Resampler>,
    ended: bool,
    /// When it last had audio (or started).
    heard: Instant,
    /// When its first audio came.
    first: Option<Instant>,
    /// It has begun playing.
    started: bool,
}

pub struct Scheduler {
    rate: u32,
    max_backlog: usize,
    gap: usize,
    prefill: usize,
    /// The call playing first, then the calls waiting, in the order they started.
    calls: VecDeque<Call>,
    finished: VecDeque<u32>,
    /// Silence still to play before the next call.
    gap_left: usize,
    played: u64,
    dropped: u64,
    /// Calls finished since asked: played (true) or dropped.
    done: Vec<(u32, bool)>,
}

impl Scheduler {
    /// Audio out at `rate`, with at most `max_delay` of it waiting.
    pub fn new(rate: u32, max_delay: Duration) -> Scheduler {
        Scheduler {
            rate,
            max_backlog: (max_delay.as_secs_f64() * rate as f64) as usize,
            gap: (GAP.as_secs_f64() * rate as f64) as usize,
            prefill: (PREFILL.as_secs_f64() * rate as f64) as usize,
            calls: VecDeque::new(),
            finished: VecDeque::new(),
            gap_left: 0,
            played: 0,
            dropped: 0,
            done: Vec::new(),
        }
    }

    /// A call started: it plays after the calls before it.
    pub fn start(&mut self, id: u32, title: String, now: Instant) {
        if self.calls.iter().any(|c| c.id == id) || self.finished.contains(&id) {
            return;
        }
        self.calls.push_back(Call { id, title, samples: VecDeque::new(), resampler: None, ended: false, heard: now, first: None, started: false });
        while self.calls.len() > MAX_CALLS {
            self.drop_waiting();
        }
    }

    /// Some of a call's audio, at `rate`.
    pub fn audio(&mut self, id: u32, rate: u32, samples: &[i16], now: Instant) {
        if rate == 0 || self.finished.contains(&id) {
            return;
        }
        if !self.calls.iter().any(|c| c.id == id) {
            self.start(id, String::new(), now);
        }
        let to = self.rate;
        let Some(call) = self.calls.iter_mut().find(|c| c.id == id) else { return };
        let mut out = Vec::with_capacity(samples.len() * to as usize / rate as usize + 1);
        if let Some(r) = call.resampler.as_mut().filter(|r| r.from() != rate) {
            r.finish(&mut out);
            call.resampler = None;
        }
        call.resampler.get_or_insert_with(|| Resampler::new(rate, to)).process(samples, &mut out);
        call.samples.extend(out);
        call.heard = now;
        call.first.get_or_insert(now);
        self.trim();
    }

    /// A call ended: it's over once what it has is played.
    pub fn end(&mut self, id: u32) {
        if let Some(call) = self.calls.iter_mut().find(|c| c.id == id) {
            if let Some(mut r) = call.resampler.take() {
                let mut tail = Vec::new();
                r.finish(&mut tail);
                call.samples.extend(tail);
            }
            call.ended = true;
        }
    }

    /// No more audio is coming (the feed is closing): every call is over once what it has is played.
    pub fn end_all(&mut self) {
        let ids: Vec<u32> = self.calls.iter().map(|c| c.id).collect();
        for id in ids {
            self.end(id);
        }
    }

    /// The next `n` samples to play, silence where there's nothing.
    pub fn pull(&mut self, n: usize, now: Instant, out: &mut Vec<i16>) {
        let target = out.len() + n;
        while out.len() < target {
            if self.gap_left > 0 {
                let k = self.gap_left.min(target - out.len());
                out.resize(out.len() + k, 0);
                self.gap_left -= k;
                continue;
            }
            let Some(call) = self.calls.front_mut() else { break };
            if !call.started {
                let waited = call.first.is_some_and(|f| now.saturating_duration_since(f) >= PREFILL);
                call.started = call.ended || waited || call.samples.len() >= self.prefill;
            }
            if call.started && !call.samples.is_empty() {
                let k = call.samples.len().min(target - out.len());
                out.extend(call.samples.drain(..k));
                continue;
            }
            if call.ended || now.saturating_duration_since(call.heard) >= QUIET_TIMEOUT {
                let call = self.calls.pop_front().expect("a call");
                self.finish(call.id);
                self.played += 1;
                self.done.push((call.id, true));
                self.gap_left = self.gap;
                continue;
            }
            // Waiting for more of the call playing.
            break;
        }
        out.resize(target, 0);
    }

    /// Whether the call is here, waiting, playing or finished.
    pub fn knows(&self, id: u32) -> bool {
        self.calls.iter().any(|c| c.id == id) || self.finished.contains(&id)
    }

    /// The call playing, once it has begun.
    pub fn playing_id(&self) -> Option<u32> {
        self.calls.front().filter(|c| c.started).map(|c| c.id)
    }

    /// The calls finished since last asked: played (true), or dropped to keep the delay down.
    pub fn take_done(&mut self) -> Vec<(u32, bool)> {
        std::mem::take(&mut self.done)
    }

    /// The title of the call playing, if one is.
    pub fn playing(&self) -> Option<&str> {
        self.calls.front().map(|c| c.title.as_str()).filter(|t| !t.is_empty())
    }

    /// Audio waiting to be played, in samples.
    pub fn backlog(&self) -> usize {
        self.calls.iter().map(|c| c.samples.len()).sum()
    }

    /// Calls played, and calls dropped to keep the delay down.
    pub fn counts(&self) -> (u64, u64) {
        (self.played, self.dropped)
    }

    /// Keep the audio waiting within the delay: drop the oldest waiting
    /// calls, then skip ahead in the one playing.
    fn trim(&mut self) {
        let mut over = self.backlog().saturating_sub(self.max_backlog);
        while over > 0 && self.calls.len() > 1 {
            self.drop_waiting();
            over = self.backlog().saturating_sub(self.max_backlog);
        }
        if over > 0
            && let Some(c) = self.calls.front_mut()
        {
            c.samples.drain(..over.min(c.samples.len()));
        }
    }

    /// Drop the oldest call waiting (not the one playing).
    fn drop_waiting(&mut self) {
        if let Some(c) = self.calls.remove(1) {
            self.finish(c.id);
            self.dropped += 1;
            self.done.push((c.id, false));
        }
    }

    fn finish(&mut self, id: u32) {
        self.finished.push_back(id);
        if self.finished.len() > FINISHED {
            self.finished.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RATE: u32 = 22050;

    fn sched() -> Scheduler {
        Scheduler::new(RATE, Duration::from_secs(60))
    }

    fn pull(s: &mut Scheduler, n: usize, now: Instant) -> Vec<i16> {
        let mut out = Vec::new();
        s.pull(n, now, &mut out);
        assert_eq!(out.len(), n);
        out
    }

    fn gap() -> usize {
        (GAP.as_secs_f64() * RATE as f64) as usize
    }

    #[test]
    fn silence_when_idle() {
        let mut s = sched();
        assert!(pull(&mut s, 1000, Instant::now()).iter().all(|&v| v == 0));
        assert_eq!(s.playing(), None);
    }

    #[test]
    fn plays_a_calls_audio() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, "Fire Dispatch (101)".into(), t);
        s.audio(1, RATE, &[5; 100], t);
        assert_eq!(s.playing(), Some("Fire Dispatch (101)"));
        let out = pull(&mut s, 150, t + PREFILL);
        assert_eq!(&out[..100], &[5; 100]);
        // Waiting for more of the call: silence, still playing it.
        assert!(out[100..].iter().all(|&v| v == 0));
        assert_eq!(s.playing(), Some("Fire Dispatch (101)"));
    }

    #[test]
    fn resamples_to_the_streams_rate() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, String::new(), t);
        s.audio(1, 8000, &[1000; 8000], t);
        let b = s.backlog();
        assert!((22000..=22050).contains(&b), "{b}");
    }

    #[test]
    fn one_call_at_a_time_in_the_order_they_started() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, "one".into(), t);
        s.start(2, "two".into(), t);
        // Audio of the two comes interleaved.
        s.audio(2, RATE, &[2; 100], t);
        s.audio(1, RATE, &[1; 100], t);
        s.audio(2, RATE, &[2; 100], t);
        s.audio(1, RATE, &[1; 100], t);
        s.end(1);
        s.end(2);
        let out = pull(&mut s, 200 + gap() + 200, t);
        assert_eq!(&out[..200], &[1; 200]);
        assert!(out[200..200 + gap()].iter().all(|&v| v == 0), "a gap between calls");
        assert_eq!(&out[200 + gap()..], &[2; 200]);
        assert_eq!(s.counts(), (1, 0));
        assert_eq!(s.playing(), Some("two"));
        pull(&mut s, 10, t);
        assert_eq!(s.counts(), (2, 0));
        assert_eq!(s.playing(), None);
    }

    #[test]
    fn a_call_that_goes_quiet_is_over() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, "lost its end".into(), t);
        s.audio(1, RATE, &[1; 10], t);
        s.start(2, "next".into(), t);
        s.audio(2, RATE, &[2; 10], t);
        // No call.end for 1. Until it times out, the next call waits.
        assert_eq!(&pull(&mut s, 20, t + PREFILL)[..10], &[1; 10]);
        assert_eq!(s.playing(), Some("lost its end"));
        let later = t + QUIET_TIMEOUT + Duration::from_millis(1);
        let out = pull(&mut s, gap() + 10, later);
        assert_eq!(&out[gap()..], &[2; 10]);
        assert_eq!(s.playing(), Some("next"));
    }

    #[test]
    fn a_call_with_no_audio_at_all_times_out() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, "silent".into(), t);
        pull(&mut s, 10, t + QUIET_TIMEOUT + Duration::from_millis(1));
        assert_eq!(s.playing(), None);
    }

    #[test]
    fn audio_without_a_start_is_a_call_too() {
        let mut s = sched();
        let t = Instant::now();
        s.audio(9, RATE, &[3; 10], t);
        assert_eq!(&pull(&mut s, 10, t + PREFILL), &[3; 10]);
    }

    #[test]
    fn audio_after_its_end_is_ignored() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, String::new(), t);
        s.audio(1, RATE, &[1; 10], t);
        s.end(1);
        pull(&mut s, 20, t);
        s.audio(1, RATE, &[1; 10], t);
        assert!(pull(&mut s, 20, t).iter().all(|&v| v == 0));
    }

    #[test]
    fn the_oldest_waiting_call_is_dropped_past_the_delay() {
        let mut s = Scheduler::new(RATE, Duration::from_secs(1));
        let t = Instant::now();
        for id in 1..=3 {
            s.start(id, id.to_string(), t);
        }
        s.audio(1, RATE, &[1; 10000], t);
        s.audio(2, RATE, &[2; 10000], t);
        // Over a second waiting: 2 goes (1 is playing), 3 stays.
        s.audio(3, RATE, &[3; 10000], t);
        assert_eq!(s.counts(), (0, 1));
        assert_eq!(s.backlog(), 20000);
        s.end(1);
        s.end(3);
        let out = pull(&mut s, 10000 + gap() + 10000, t);
        assert_eq!(out[10000 + gap()], 3);
    }

    #[test]
    fn a_call_longer_than_the_delay_skips_ahead() {
        let mut s = Scheduler::new(RATE, Duration::from_secs(1));
        let t = Instant::now();
        s.start(1, String::new(), t);
        s.audio(1, RATE, &[1; 22050], t);
        s.audio(1, RATE, &[2; 100], t);
        assert_eq!(s.backlog(), 22050);
        assert_eq!(pull(&mut s, 22050, t)[22049], 2);
    }

    #[test]
    fn calls_that_never_play_are_bounded() {
        let mut s = sched();
        let t = Instant::now();
        for id in 0..1000 {
            s.start(id, String::new(), t);
        }
        assert_eq!(s.counts().1, 1000 - MAX_CALLS as u64);
    }

    #[test]
    fn a_call_waits_for_some_audio_before_it_plays() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, String::new(), t);
        let prefill = (PREFILL.as_secs_f64() * RATE as f64) as usize;
        s.audio(1, RATE, &vec![1; prefill - 1], t);
        assert!(pull(&mut s, 10, t).iter().all(|&v| v == 0), "too little yet");
        s.audio(1, RATE, &[1], t);
        assert_eq!(pull(&mut s, 10, t), [1; 10], "enough");
        // Once it plays, it plays what it has: no waiting again mid-call.
        assert_eq!(s.backlog(), prefill - 10);
    }

    #[test]
    fn a_short_call_plays_once_it_ends_or_has_waited() {
        let t = Instant::now();
        let mut s = sched();
        s.audio(1, RATE, &[1; 10], t);
        s.end(1);
        assert_eq!(&pull(&mut s, 10, t), &[1; 10]);
        let mut s = sched();
        s.audio(1, RATE, &[1; 10], t);
        assert!(pull(&mut s, 10, t + PREFILL / 2).iter().all(|&v| v == 0));
        assert_eq!(&pull(&mut s, 10, t + PREFILL), &[1; 10]);
    }

    #[test]
    fn at_the_end_everything_waiting_plays() {
        let mut s = sched();
        let t = Instant::now();
        // The call playing has nothing more yet, and hasn't ended; another waits behind it.
        s.audio(1, RATE, &[1; 10], t);
        s.audio(2, RATE, &[2; 10], t);
        pull(&mut s, 10, t + PREFILL);
        s.end_all();
        let out = pull(&mut s, gap() + 10, t + PREFILL);
        assert_eq!(&out[gap()..], &[2; 10]);
        assert_eq!(s.backlog(), 0);
        pull(&mut s, 1, t + PREFILL);
        assert_eq!(s.counts(), (2, 0));
    }

    #[test]
    fn says_which_calls_it_finished_and_how() {
        let mut s = Scheduler::new(RATE, Duration::from_secs(1));
        let t = Instant::now();
        for id in 1..=3 {
            s.start(id, String::new(), t);
        }
        s.audio(1, RATE, &[1; 10000], t);
        assert_eq!(s.playing_id(), None, "not begun");
        s.audio(2, RATE, &[2; 10000], t);
        s.audio(3, RATE, &[3; 10000], t);
        assert_eq!(s.take_done(), [(2, false)]);
        s.end_all();
        pull(&mut s, 5000, t);
        assert_eq!(s.playing_id(), Some(1));
        pull(&mut s, 20000 + gap(), t);
        assert_eq!(s.take_done(), [(1, true), (3, true)]);
        assert!(s.take_done().is_empty());
    }

    #[test]
    fn knows_its_calls() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, String::new(), t);
        assert!(s.knows(1) && !s.knows(2));
        s.end(1);
        pull(&mut s, 10, t);
        // Finished, and still known (so late audio isn't a new call elsewhere).
        assert!(s.knows(1));
    }

    #[test]
    fn a_calls_tail_is_let_out_at_its_end() {
        let mut s = sched();
        let t = Instant::now();
        s.start(1, String::new(), t);
        s.audio(1, 8000, &[1000; 800], t);
        let before = s.backlog();
        s.end(1);
        assert!(s.backlog() > before);
    }
}
