//! Changing a call's sample rate (8 kHz from the recorder) to the stream's
//! (22.05 kHz for Broadcastify): a polyphase windowed-sinc filter, kept
//! across chunks so a call resamples the same however it's sliced.

/// Filter taps for each input sample (per phase), when upsampling.
const TAPS: usize = 24;

pub struct Resampler {
    from: u32,
    /// Up by `up`, filter, down by `down`: the rates' ratio, reduced.
    up: usize,
    down: usize,
    taps: usize,
    /// The filter, by phase: `coefs[phase * taps + j]`. Empty for the same rate.
    coefs: Vec<f32>,
    /// The input still needed, oldest first.
    hist: Vec<f32>,
    /// The next output's place on the upsampled timeline, from `hist[0]`.
    pos: usize,
}

impl Resampler {
    pub fn new(from: u32, to: u32) -> Resampler {
        let g = gcd(from, to);
        let (up, down) = ((to / g) as usize, (from / g) as usize);
        if up == down {
            return Resampler { from, up: 1, down: 1, taps: 1, coefs: Vec::new(), hist: Vec::new(), pos: 0 };
        }
        // Cut off below the lower rate's Nyquist frequency; downsampling needs a longer filter for that.
        let taps = TAPS * (from as usize).div_ceil(from.min(to) as usize);
        let len = up * taps;
        let wc = 0.45 * from.min(to) as f64 / (from as f64 * up as f64);
        let c = (len - 1) as f64 / 2.0;
        let proto: Vec<f64> = (0..len)
            .map(|n| {
                let x = n as f64 - c;
                let sinc = if x == 0.0 { 1.0 } else { (std::f64::consts::PI * 2.0 * wc * x).sin() / (std::f64::consts::PI * 2.0 * wc * x) };
                let t = 2.0 * std::f64::consts::PI * n as f64 / (len - 1) as f64;
                sinc * (0.42 - 0.5 * t.cos() + 0.08 * (2.0 * t).cos())
            })
            .collect();
        let mut coefs = vec![0f32; len];
        for p in 0..up {
            // Each phase passes a steady level unchanged.
            let sum: f64 = (0..taps).map(|j| proto[p + j * up]).sum();
            for j in 0..taps {
                coefs[p * taps + j] = (proto[p + j * up] / sum) as f32;
            }
        }
        Resampler { from, up, down, taps, coefs, hist: vec![0.0; taps - 1], pos: (taps - 1) * up }
    }

    /// The rate it takes.
    pub fn from(&self) -> u32 {
        self.from
    }

    /// Resample `input`, appending to `out`.
    pub fn process(&mut self, input: &[i16], out: &mut Vec<i16>) {
        if self.coefs.is_empty() {
            out.extend_from_slice(input);
            return;
        }
        self.hist.extend(input.iter().map(|&v| v as f32));
        loop {
            let i = self.pos / self.up;
            if i >= self.hist.len() {
                break;
            }
            let phase = &self.coefs[(self.pos % self.up) * self.taps..][..self.taps];
            let y: f32 = phase.iter().enumerate().map(|(j, c)| c * self.hist[i - j]).sum();
            out.push(y.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16);
            self.pos += self.down;
        }
        let keep_from = (self.pos / self.up).saturating_sub(self.taps - 1).min(self.hist.len());
        self.hist.drain(..keep_from);
        self.pos -= keep_from * self.up;
    }

    /// The last of the audio, still in the filter: call at the end of a call.
    pub fn finish(&mut self, out: &mut Vec<i16>) {
        if !self.coefs.is_empty() {
            self.process(&vec![0; self.taps / 2], out);
        }
    }
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 { a.max(1) } else { gcd(b, a % b) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(hz: f64, rate: u32, n: usize, amp: f64) -> Vec<i16> {
        (0..n).map(|i| (amp * (2.0 * std::f64::consts::PI * hz * i as f64 / rate as f64).sin()) as i16).collect()
    }

    /// Strength of `hz` in `x` (Goertzel), normalized to the amplitude of a sine.
    fn strength(x: &[i16], hz: f64, rate: u32) -> f64 {
        let w = 2.0 * std::f64::consts::PI * hz / rate as f64;
        let (mut s1, mut s2) = (0.0, 0.0);
        for &v in x {
            let s = v as f64 + 2.0 * w.cos() * s1 - s2;
            s2 = s1;
            s1 = s;
        }
        (s1 * s1 + s2 * s2 - 2.0 * w.cos() * s1 * s2).sqrt() * 2.0 / x.len() as f64
    }

    fn all(r: &mut Resampler, input: &[i16]) -> Vec<i16> {
        let mut out = Vec::new();
        r.process(input, &mut out);
        out
    }

    #[test]
    fn makes_the_right_number_of_samples() {
        let mut r = Resampler::new(8000, 22050);
        let out = all(&mut r, &vec![0; 8000]);
        assert!((22040..=22050).contains(&out.len()), "{}", out.len());
    }

    #[test]
    fn steady_level_passes_through() {
        let mut r = Resampler::new(8000, 22050);
        let out = all(&mut r, &vec![1000; 4000]);
        for &v in &out[200..] {
            assert!((995..=1005).contains(&v), "{v}");
        }
    }

    #[test]
    fn a_tone_keeps_its_pitch_and_level() {
        let mut r = Resampler::new(8000, 22050);
        let out = all(&mut r, &tone(1000.0, 8000, 8000, 10000.0));
        let body = &out[1000..21000];
        let at = strength(body, 1000.0, 22050);
        assert!((9500.0..10500.0).contains(&at), "{at}");
        // No image of it above the old Nyquist frequency (8000 - 1000 Hz).
        assert!(strength(body, 7000.0, 22050) < 100.0);
    }

    #[test]
    fn chunks_make_the_same_audio_as_the_whole() {
        let input = tone(440.0, 8000, 1600, 8000.0);
        let whole = all(&mut Resampler::new(8000, 22050), &input);
        let mut r = Resampler::new(8000, 22050);
        let mut chunked = Vec::new();
        for c in input.chunks(160) {
            r.process(c, &mut chunked);
        }
        assert_eq!(whole, chunked);
    }

    #[test]
    fn finish_lets_out_the_tail() {
        let mut r = Resampler::new(8000, 22050);
        let mut out = all(&mut r, &vec![1000; 800]);
        let before = out.len();
        r.finish(&mut out);
        assert!(out.len() > before);
        assert!((2206..2260).contains(&out.len()), "{}", out.len());
    }

    #[test]
    fn same_rate_is_untouched() {
        let mut r = Resampler::new(22050, 22050);
        let input = tone(440.0, 22050, 500, 8000.0);
        assert_eq!(all(&mut r, &input), input);
    }

    #[test]
    fn downsampling_works_too() {
        let mut r = Resampler::new(44100, 22050);
        let out = all(&mut r, &tone(1000.0, 44100, 44100, 10000.0));
        assert!((22040..=22050).contains(&out.len()));
        assert!((9500.0..10500.0).contains(&strength(&out[1000..21000], 1000.0, 22050)));
    }
}
