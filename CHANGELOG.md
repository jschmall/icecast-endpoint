# Changelog

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
  doesn't allow.

### Changed

- With **Show the talkgroup as the title** on, a stereo feed's title is the call that began
  playing last. Mono feeds work as before.
- **Longest delay** applies to each side of a stereo feed on its own.
- The README says that **Show the talkgroup as the title** turns on alpha tags for the stream.

### Upgrading

- Nothing to change: feeds without talkgroups on a side stay mono, and stream exactly as in 0.1.0.
- Before making a Broadcastify feed stereo, set Bitrate to 32 (Broadcastify's stereo bitrate) and
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
