# Icecast Endpoint

Streams your recorded calls live to an Icecast server, such as a
[Broadcastify](https://www.broadcastify.com) feed. It's a plugin for
[Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro).

Each feed is one continuous audio stream, like a scanner's speaker output. Calls
play as they're recorded, and there's silence between them. You choose which
systems and talkgroups each feed plays. A feed can also be stereo, with
talkgroups panned left and right so two calls can be heard at once.

> This plugin is for Broadcastify's **live audio feeds**. To upload individual
> calls to **Broadcastify Calls**, use the
> [Broadcastify plugin](https://github.com/TrunkRecorder/trunk-plugin-broadcastify) instead.

## What you need

- Trunk Recorder Pro.
- An Icecast mount to stream to: its server, port, mount and source password.
  For a Broadcastify feed, these are on your feed's **Technical** tab.

Nothing else to install. The MP3 encoder is built in.

## Getting started

1. **Install the plugin.** Download the build for your computer from the
   [releases page](https://github.com/jschmall/icecast-endpoint/releases) and
   unpack it. In the recorder, open the **Plugins** page, press **Add from a
   file**, and choose the `icecast-endpoint` program you unpacked.
2. **Set it up.** Press **Set up** on the plugin's card, then **Add feed**.
   Fill in **Server**, **Port**, **Mount** and **Password**. For a
   Broadcastify feed, copy each one from your feed's Technical tab.
3. **Choose what it plays** (optional). Pick a **System**, and list the
   **Talkgroups** the feed is for. Left empty, the feed plays every call. To
   hear talkgroups side by side, list some under **Talkgroups on the left**
   and **Talkgroups on the right** (see [Stereo](#stereo)).
4. **Turn it on**, and start recording.

The plugin's card shows **streaming to** your feed once it's connected. To
hear it, open your feed's page on Broadcastify, or
`http://<server>:<port>/<mount>` for your own Icecast server.

## Settings

Each feed has these settings. Only Server, Mount and Password have to be
filled in.

| Setting | |
|---|---|
| Name | What to call the feed in the recorder's log. |
| Server | The server's host name, such as `audio9.broadcastify.com`. |
| Port | The server's port. Default: 80. |
| Mount | The mount point, such as `/abcd1234`. |
| Username | Leave it as `source` unless your server says otherwise. |
| Password | The mount's source password. |
| Protocol | **Automatic** works with any Icecast server. Choose PUT or SOURCE only if your server needs one of them. |
| Bitrate (kbps) | Default: 16, what Broadcastify recommends for a mono feed (32 for stereo). Other choices: 8, 24, 32, 40, 48, 64, 80, 96, 112 and 128. |
| Sample rate (Hz) | Default: 22050, what Broadcastify recommends. Other choices: 16000, 24000, 32000, 44100 and 48000. |
| Volume | How loud the calls play: 1 is as recorded, 2 twice as loud, 0.5 half as loud, from 0.1 to 10. If your recorder's audio is quiet, try 2 or 3. Loud parts are clipped, so raise it only as far as you need. Default: 1. |
| System | The system whose calls the feed plays. Empty: every system. |
| Talkgroups | The talkgroups the feed plays, by number. `*` matches any digits and `?` any one digit, so `507*` matches 5071 and 50712. Empty: every talkgroup, unless talkgroups are on the left or right (see [Stereo](#stereo)). |
| Leave out talkgroups | Talkgroups the feed never plays, written the same way. |
| Talkgroups on the left, Talkgroups on the right | Talkgroups heard in only one ear, written the same way. See [Stereo](#stereo). |
| Show the talkgroup as the title | Turns on alpha tags for the stream: its title shows the talkgroup playing, such as "Fire Dispatch (101)", using the recorder's alpha tag for that talkgroup. Off by default. Not every server accepts titles. |
| Stream name, Description, Genre, Website | What listeners see about the stream. Stream name defaults to the feed's Name. |
| List publicly | Asks the server to list the stream in its public directory. Leave it off for Broadcastify. |
| Longest delay (seconds) | Calls that happen at the same time play one after another (on a stereo feed, each side on its own). When more than this much audio is waiting, the oldest waiting call is dropped. Default: 60. For a feed meant to sound live, 10 to 15 keeps it close to real time. |

You can add as many feeds as you like, each with its own mount. A system can
also have feeds of its own, set on the system's card under **Systems**. Those
feeds play only that system's calls.

### Stereo

A feed can play several talkgroups at once, panned left and right, like two
scanners wired to the two sides of one input. List talkgroups under
**Talkgroups on the left** and **Talkgroups on the right**, and the feed
becomes stereo:

- A call on the left is heard only in the left ear, and one on the right only
  in the right. Other talkgroups play in the center, in both ears.
- The left, the center and the right each play their own calls one at a
  time, and all three play at once. A left call and a right call that happen
  together are both heard live; two left calls take turns.
- Talkgroups on a side are played even when they're not under **Talkgroups**.
  When Talkgroups is empty, the feed plays only the talkgroups on the sides.
  To play every other talkgroup in the center as well, put `*` in Talkgroups.
- A talkgroup on both sides plays in the center. **Leave out talkgroups**
  still wins over everything.

With **Show the talkgroup as the title** on, the title is the call that began
playing last.

### Broadcastify

The defaults, 16 kbps mono MP3 at 22050 Hz, are what Broadcastify recommends
for a feed, and for a stereo feed (with talkgroups on the left or right) it
recommends 32 kbps at 22050 Hz, so set Bitrate to 32. These are
recommendations, not limits: Broadcastify's servers take higher settings too.
If your feed's Technical tab lists other values, use those. If you're moving
a feed from another streaming setup, such as Liquidsoap or a scanner's
software, you can keep the bitrate and sample rate it used.

A stereo feed may also need to be set up as stereo on Broadcastify's side.
Check your feed's settings there before you switch an existing mono feed.

## When something's wrong

The plugin's card, and the recorder's log, say what's wrong:

| Message | What to do |
|---|---|
| wrong username or password | Check the Password (and Username) against your feed's Technical tab. |
| the server refused the mount (in use by another source…) | Something else is already streaming to that mount, such as an old scanner setup. Stop it, or use another mount. |
| there's no such mount | Check the Mount. |
| can't connect to … | Check the Server and Port, and your internet connection. The plugin keeps retrying on its own. |

If the connection drops, the plugin reconnects by itself, waiting a little
longer after each try, up to a minute.

## What it sends where

Each feed sends audio of the calls it plays, as MP3, to the server you set.
With **Show the talkgroup as the title** on, it also sends the talkgroup's name
and number. Nothing else leaves your computer, and passwords never appear in
the log.

## Building from source

```sh
cargo build --release
cargo test
```

To test against a local Icecast server in Docker, run `scripts/icecast.sh up`,
then `cargo test -- --ignored`.

## License

MIT. The plugin includes the [LAME](https://lame.sourceforge.io) MP3 encoder,
which is under the LGPL.
