# Changelog

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
