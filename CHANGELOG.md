# Changelog

## [0.4.0]

Sent, failed and skipped counts for each call on the Plugins page.

### Added

- The plugin reports what became of each recorded call, so the recorder's health page counts calls
  sent, failed and skipped, as it does for upload plugins:
  - **Sent:** played in full on every feed it's on, while the feed was connected.
  - **Failed:** dropped because more than the Longest delay was waiting, or played while the feed
    couldn't reach the server. The message names the feed and the reason.
  - **Skipped:** on no feed (a system or talkgroup no feed plays), encrypted, or not heard live
    (recorded before the plugin started, or replayed with `trunk-pro plugin run`).
- A call is counted once it has played, so a call waiting behind others counts a little after it's
  recorded. A call on two feeds counts once. A call heard on several sites counts as the copy the
  recorder kept.

### Changed

- The plugin now also subscribes to `call.concluded`, to know each recorded call's files. It asks
  for no extra audio formats, so the recorder does no extra work for it.

### Upgrading

- Nothing to change: there are no new settings.

## [0.3.0]

A volume setting, steadier audio, and corrected Broadcastify advice.

### Added

- **Volume**, a new setting on each feed: how loud the calls play, from 0.1 to 10. 1 plays them as
  recorded (the default, and what earlier versions did), 2 twice as loud, 0.5 half. On a stereo
  feed it applies to both sides. Loud parts are clipped.

### Changed

- Each call now waits for 150 ms of its audio (or 150 ms after its first audio) before it plays.
  Audio that reaches the plugin a little late no longer leaves short silences inside a call; before,
  a call could break up for a few tens of milliseconds near its start. Calls start 150 ms later.
- The README and the settings form say what Broadcastify *recommends* (16 kbps mono at 22050 Hz, or
  32 kbps for stereo) instead of what it requires: its servers take higher settings too. The README
  suggests a Longest delay of 10 to 15 seconds for a feed meant to sound live.

### Fixed

- Stopping the plugin while a call was still going, with another call waiting behind it, could make
  the plugin send up to minutes of silence before stopping, or run past the time the recorder
  allows and be killed. Now everything waiting plays, and the stream closes.

### Upgrading

- Nothing to change: Volume starts at 1, and feeds sound as before apart from the 150 ms.

## [0.2.0]

Stereo feeds, with talkgroups panned left and right.

### Added

- **Talkgroups on the left** and **Talkgroups on the right**, two new settings on each feed,
  written like Talkgroups (`507*`). Listing any talkgroup on either side makes the feed stereo.
- On a stereo feed, calls on the left are heard only in the left ear, calls on the right only in
  the right, and every other talkgroup in both ears.
- The left, the center and the right each play their own calls one at a time, and all three play
  at once. A left call and a right call that happen together are both heard live, like two
  scanners wired to the two sides of one input.
- Talkgroups on a side are always played, even when they're not under Talkgroups. With
  Talkgroups empty, a stereo feed plays only the talkgroups on its sides; put `*` in Talkgroups to
  play the rest in the center too. A talkgroup on both sides plays in the center, and Leave out
  talkgroups still wins over everything.
- Stereo is true stereo (left and right encoded separately), not joint stereo, which Broadcastify
  recommends against.

### Changed

- With **Show the talkgroup as the title** on, a stereo feed's title is the call that began
  playing last. Mono feeds work as before.
- **Longest delay** applies to each side of a stereo feed on its own.
- The README says that **Show the talkgroup as the title** turns on alpha tags for the stream.

### Upgrading

- Nothing to change: feeds without talkgroups on a side stay mono, and stream exactly as in 0.1.0.
- Before making a Broadcastify feed stereo, set Bitrate to 32 (Broadcastify's recommended stereo bitrate) and
  check that the feed is set up as stereo on Broadcastify's side.

## [0.1.0]

First release.

- Streams live call audio as MP3 to one or more Icecast mounts, such as Broadcastify live feeds.
- Plays silence between calls, so the stream stays connected.
- Plays calls that happen at the same time one after another. When more audio is waiting than the
  longest delay you set, the oldest waiting call is dropped.
- Lets you pick each feed's system and talkgroups, with allow and leave-out patterns. A system can
  also have feeds of its own.
- Connects with PUT (Icecast 2.4 and later) or SOURCE, and picks one automatically by default.
- Can show the talkgroup playing as the stream's title.
- Reconnects on its own, waiting up to a minute between tries. The plugin's card shows each feed's
  state and counts calls played and dropped.
- Built-in MP3 encoder (LAME). There's nothing else to install.
