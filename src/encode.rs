//! MP3 encoding with LAME: constant bitrate, mono or stereo. The one place
//! the encoder crate is used.

use std::num::NonZeroU32;

use mp3lame_encoder::{Bitrate, Builder, DualPcm, Encoder, EncoderInput, FlushGap, Mode, MonoPcm, Quality};

/// The sample rates MP3 has.
pub const RATES: [u32; 9] = [8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000];

pub struct Mp3 {
    enc: Encoder,
    out: Vec<u8>,
    /// It has had audio (LAME complains at flushing without).
    fed: bool,
}

impl Mp3 {
    /// An encoder for audio at `rate`, to MP3 at `kbps`, mono or (not joint) stereo.
    /// Errors say what's wrong with the settings.
    pub fn new(rate: u32, kbps: u32, stereo: bool) -> Result<Mp3, String> {
        if !RATES.contains(&rate) {
            let rates: Vec<String> = RATES.iter().map(|r| r.to_string()).collect();
            return Err(format!("a sample rate of {rate} Hz isn't one MP3 has ({})", rates.join(", ")));
        }
        let brate = bitrate(rate, kbps).ok_or_else(|| {
            let (lo, hi) = if rate >= 32000 { (32, 320) } else { (8, 160) };
            format!("{kbps} kbps isn't an MP3 bitrate at {rate} Hz (from {lo} to {hi}, such as 16, 24, 32, 48, 64 or 128)")
        })?;
        let e = |what: &'static str| move |err: mp3lame_encoder::BuildError| format!("the MP3 encoder refused the {what}: {err:?}");
        let enc = Builder::new()
            .ok_or("the MP3 encoder couldn't start")?
            .with_num_channels(if stereo { 2 } else { 1 })
            .map_err(e("channels"))?
            .with_sample_rate(rate)
            .map_err(e("sample rate"))?
            .with_output_sample_rate(NonZeroU32::new(rate))
            .map_err(e("sample rate"))?
            .with_brate(brate)
            .map_err(e("bitrate"))?
            .with_mode(if stereo { Mode::Stereo } else { Mode::Mono })
            .map_err(e("mode"))?
            .with_quality(Quality::NearBest)
            .map_err(e("quality"))?
            .with_to_write_vbr_tag(false)
            .map_err(e("settings"))?
            .build()
            .map_err(e("settings"))?;
        Ok(Mp3 { enc, out: Vec::new(), fed: false })
    }

    /// Encode mono `pcm`, returning the MP3 bytes ready so far (often none: LAME works in frames).
    pub fn encode(&mut self, pcm: &[i16]) -> Result<&[u8], String> {
        self.encode_any(pcm.len(), MonoPcm(pcm))
    }

    /// Encode stereo audio, the same length each side.
    pub fn encode_stereo(&mut self, left: &[i16], right: &[i16]) -> Result<&[u8], String> {
        self.encode_any(left.len().min(right.len()), DualPcm { left, right })
    }

    fn encode_any(&mut self, n: usize, pcm: impl EncoderInput) -> Result<&[u8], String> {
        self.out.clear();
        self.fed |= n > 0;
        self.out.reserve(mp3lame_encoder::max_required_buffer_size(n));
        self.enc.encode_to_vec(pcm, &mut self.out).map_err(|e| format!("MP3 encoding failed: {e:?}"))?;
        Ok(&self.out)
    }

    /// The rest of the MP3, at the end of a stream: what LAME holds, padded out to whole frames.
    pub fn flush(&mut self) -> Vec<u8> {
        if !std::mem::take(&mut self.fed) {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(7200);
        let _ = self.enc.flush_to_vec::<FlushGap>(&mut out);
        out
    }
}

/// LAME's bitrate for `kbps`, if MP3 has it at `rate` (MPEG-1 at 32 kHz and up, MPEG-2 below).
fn bitrate(rate: u32, kbps: u32) -> Option<Bitrate> {
    use Bitrate::*;
    let b = match kbps {
        8 => Kbps8,
        16 => Kbps16,
        24 => Kbps24,
        32 => Kbps32,
        40 => Kbps40,
        48 => Kbps48,
        64 => Kbps64,
        80 => Kbps80,
        96 => Kbps96,
        112 => Kbps112,
        128 => Kbps128,
        160 => Kbps160,
        192 => Kbps192,
        224 => Kbps224,
        256 => Kbps256,
        320 => Kbps320,
        _ => return None,
    };
    let ok = if rate >= 32000 { (32..=320).contains(&kbps) } else { kbps <= 160 };
    ok.then_some(b)
}

/// An MP3 frame's header, for tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Frame {
    pub rate: u32,
    pub kbps: u32,
    pub mono: bool,
    pub joint: bool,
    pub len: usize,
}

/// The frames in `b`, from the first frame header on; `Err` where the run of frames breaks.
#[cfg(test)]
pub fn frames(b: &[u8]) -> Result<Vec<Frame>, String> {
    let header = |h: &[u8]| -> Option<Frame> {
        if h.len() < 4 || h[0] != 0xFF || h[1] & 0xE0 != 0xE0 || (h[1] >> 1) & 3 != 1 {
            return None;
        }
        let version = (h[1] >> 3) & 3;
        let (kbps_table, rates): (&[u32], [u32; 3]) = match version {
            3 => (&[0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320], [44100, 48000, 32000]),
            2 => (&[0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160], [22050, 24000, 16000]),
            0 => (&[0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160], [11025, 12000, 8000]),
            _ => return None,
        };
        let kbps = *kbps_table.get((h[2] >> 4) as usize)?;
        let rate = *rates.get(((h[2] >> 2) & 3) as usize)?;
        if kbps == 0 {
            return None;
        }
        let pad = ((h[2] >> 1) & 1) as usize;
        let per = if version == 3 { 144 } else { 72 };
        Some(Frame { rate, kbps, mono: h[3] >> 6 == 3, joint: h[3] >> 6 == 1, len: (per * kbps * 1000 / rate) as usize + pad })
    };
    let mut at = (0..b.len()).find(|&i| header(&b[i..]).is_some()).ok_or("no MP3 frame")?;
    let mut out = Vec::new();
    while at + 4 <= b.len() {
        let f = header(&b[at..]).ok_or_else(|| format!("no frame header at byte {at} after {} frames", out.len()))?;
        out.push(f);
        at += f.len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_all(m: &mut Mp3, pcm: &[i16]) -> Vec<u8> {
        let mut out = Vec::new();
        for c in pcm.chunks(441) {
            out.extend_from_slice(m.encode(c).unwrap());
        }
        out.extend(m.flush());
        out
    }

    #[test]
    fn broadcastifys_format() {
        let mut m = Mp3::new(22050, 16, false).unwrap();
        let out = encode_all(&mut m, &vec![0; 22050 * 2]);
        let f = frames(&out).unwrap();
        // 1152 / 2 = 576 samples a frame at MPEG-2: 2 s is about 76 frames.
        assert!((70..=82).contains(&f.len()), "{}", f.len());
        assert!(f.iter().all(|f| f.rate == 22050 && f.kbps == 16 && f.mono), "{:?}", f[0]);
        // Constant bitrate: about 2 KB a second.
        assert!((3600..4400).contains(&out.len()), "{}", out.len());
    }

    #[test]
    fn audio_is_encoded_too() {
        let mut m = Mp3::new(22050, 16, false).unwrap();
        let tone: Vec<i16> = (0..22050).map(|i| ((i as f64 * 0.2).sin() * 8000.0) as i16).collect();
        let f = frames(&encode_all(&mut m, &tone)).unwrap();
        assert!(f.iter().all(|f| f.kbps == 16));
    }

    #[test]
    fn other_rates_and_bitrates() {
        for (rate, kbps) in [(16000, 8), (24000, 32), (44100, 64), (48000, 128)] {
            let mut m = Mp3::new(rate, kbps, false).unwrap();
            let f = frames(&encode_all(&mut m, &vec![0; rate as usize])).unwrap();
            assert_eq!((f[0].rate, f[0].kbps), (rate, kbps));
        }
    }

    #[test]
    fn stereo() {
        let mut m = Mp3::new(22050, 32, true).unwrap();
        let tone: Vec<i16> = (0..22050).map(|i| ((i as f64 * 0.2).sin() * 8000.0) as i16).collect();
        let mut out = Vec::new();
        for c in tone.chunks(441) {
            out.extend_from_slice(m.encode_stereo(c, &vec![0; c.len()]).unwrap());
        }
        out.extend(m.flush());
        let f = frames(&out).unwrap();
        assert!(f.iter().all(|f| f.rate == 22050 && f.kbps == 32 && !f.mono && !f.joint), "{:?}", f[0]);
        // Broadcastify's 16 kbps works in stereo too.
        assert!(Mp3::new(22050, 16, true).is_ok());
    }

    #[test]
    fn a_short_stream_still_ends_in_whole_frames() {
        let mut m = Mp3::new(22050, 16, false).unwrap();
        let mut out = m.encode(&[1000; 100]).unwrap().to_vec();
        out.extend(m.flush());
        assert!(!frames(&out).unwrap().is_empty());
    }

    #[test]
    fn nothing_to_flush_without_audio() {
        let mut m = Mp3::new(22050, 16, false).unwrap();
        assert!(m.encode(&[]).unwrap().is_empty());
        assert!(m.flush().is_empty());
    }

    #[test]
    fn unsupported_settings_say_so() {
        assert!(Mp3::new(22050, 17, false).err().unwrap().contains("17 kbps"));
        assert!(Mp3::new(22050, 320, false).err().unwrap().contains("320 kbps"));
        assert!(Mp3::new(11000, 16, false).err().unwrap().contains("11000"));
        // MPEG-1 rates start at 32 kbps.
        assert!(Mp3::new(44100, 16, false).is_err());
    }
}
