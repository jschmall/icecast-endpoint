# icecast-endpoint

A plugin for Trunk Recorder Pro (https://github.com/TrunkRecorder/trunk-recorder-pro), written in Rust.

## Purpose

Stream recorded audio to an Icecast-compatible server as a continuous source feed. The primary target is
Broadcastify's traditional feed method (an Icecast source connection), NOT Broadcastify Calls, which is a
separate per-call upload API already covered by trunk-plugin-broadcastify. Any standard Icecast server
should also work as an endpoint.

## How plugins work (constraints that must not be violated)

- A plugin is a separate process. The recorder starts it, sends events, and stops it.
- Transport is JSON lines over stdin/stdout. The plugin's stdout belongs to the protocol.
  NEVER use println! or write anything else to stdout. Use the SDK's Host logging or eprintln!.
- Plugins only observe. They cannot change what the recorder records.
- Every Plugin method runs on the single thread that reads recorder messages, one at a time.
  Return quickly. Hand network, encoding, and subprocess work to our own threads.
- The SDK is the `trunk-recorder-plugin` crate. API reference: https://docs.rs/trunk-recorder-plugin
- Settings are a config struct deriving JsonSchema. The recorder draws a settings form from it.
  Only a subset of JSON Schema is supported; see docs/settings.md.

## Source of truth

Read the relevant doc before writing code. Do not guess event names, field names, or protocol details.

- docs/getting-started.md
- docs/how-plugins-work.md (lifecycle, threads, failures)
- docs/events.md (every subscribable event, including live audio)
- docs/settings.md
- docs/audio.md (audio formats, fallbacks)
- docs/uploading.md (keys, retries, requests)
- docs/testing.md
- docs/releasing.md
- docs/protocol.md
- Closest reference plugin: https://github.com/TrunkRecorder/trunk-plugin-simplestream (live audio streaming)
- Other references: trunk-plugin-broadcastify (Calls API, not what we are building), trunk-plugin-openmhz,
  trunk-plugin-rdioscanner, trunk-plugin-upload-script (all under github.com/TrunkRecorder)

## Design notes and open questions

Resolve these from the docs and the Icecast and Broadcastify specs before implementing. Do not assume.

- Input: use the live audio event (see docs/events.md and docs/audio.md). Confirm sample rate, channel
  count, and sample format, and what happens between calls (no audio is delivered when idle).
- Icecast source protocol: confirm which handshake to support (HTTP PUT as in Icecast 2.4+, and/or the
  legacy SOURCE method), authentication (username and password), mount point, and required headers
  (Content-Type, ice-* metadata headers).
- Encoding: Icecast streams a compressed format, typically MP3. Decide between a pure-Rust encoder and
  shelling out to an external encoder, weighing cross-platform release builds (Linux, macOS, Windows).
  Ask before adding a dependency.
- Continuity: an Icecast source is a continuous stream, but the recorder only produces audio during calls.
  Design how silence is generated between calls so the connection stays alive, and confirm Broadcastify's
  requirements for this.
- Broadcastify feed requirements (bitrate, sample rate, mono or stereo, metadata, feed ID and credentials):
  verify against their current documentation before hardcoding any value.
- Scope of a feed: one feed per plugin instance, or filtering by system and talkgroup. The SDK includes a
  TalkgroupFilter for Trunk Recorder style allow and deny patterns.
- Resilience: reconnect with backoff on disconnect, never block the message-reading thread, bound any
  audio buffer, and report state through the SDK's health reporting.
- Metadata: optionally update the stream title with the current talkgroup or alpha tag, if supported by
  the target server.

## Commands

```bash
cargo build
cargo test
cargo fmt --all
cargo clippy --all-targets -- -D warnings
./target/debug/icecast-endpoint --describe
trunk-pro plugin run ./target/debug/icecast-endpoint ~/TrunkRecorderPro --limit 5
```

`trunk-pro plugin run` replays already-recorded calls through the plugin without live recording.
Use the SDK's `testing` module, including its mock web server, for unit tests. For integration testing,
run a local Icecast server and point the plugin at it.

## Conventions

- Follow rustfmt.toml in the repo. Run fmt and clippy before every commit.
- Every new event handler and settings field gets a unit test.
- Never log secrets (Icecast source passwords, stream keys). Redact them in all log and health output.
- Handle missing or empty fields defensively.
- Report problems through the SDK's health reporting, not panics. A panic kills the plugin process.
- Keep the SDK's API_VERSION compatibility behavior; do not bypass it.

## Release

- Update the version in Cargo.toml, then `git tag vX.Y.Z && git push --tags`.
- The release workflow builds Linux, macOS, and Windows binaries.
- Before the first release: delete docs/ (template guide), rewrite README.md for end users.
- Registry submission: see docs/registry.md.

## Working agreement

- Before implementing a feature, state which docs and events it depends on.
- Prefer small, tested increments. Run `cargo test` after each.
- Ask before adding dependencies, and before changing the settings schema once released.
