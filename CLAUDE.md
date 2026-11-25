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
  Only a subset of JSON Schema is supported; see Settings in writing-plugins.md (below).

## Source of truth

Read the relevant doc before writing code. Do not guess event names, field names, or protocol details.

The guide is in the app's repo, https://github.com/TrunkRecorder/trunk-recorder-pro (read it at the release
you build for, e.g. `v0.5.1`, rather than `main`). The template's own docs/ are older copies: don't use them.

- docs/plugins/writing-plugins.md: how a plugin runs (manifest, threads, failures), settings, audio (formats),
  testing, releasing, publishing to the registry
- docs/developer/plugins/events.md: every subscribable event, including live audio, and their order and loss
- docs/developer/plugins/protocol.md: the wire protocol, settings schemas
- docs/developer/plugins/uploading.md: uploaders (`CallQueue`)
- Closest reference plugin: https://github.com/TrunkRecorder/trunk-plugin-simplestream (live audio streaming)
- Other references: trunk-plugin-broadcastify (Calls API, not what we are building), trunk-plugin-openmhz,
  trunk-plugin-rdioscanner, trunk-plugin-upload-script (all under github.com/TrunkRecorder)

## Design notes and open questions

Resolve these from the docs and the Icecast and Broadcastify specs before implementing. Do not assume.

- Input: use the live audio event (see events.md, and Audio in writing-plugins.md). Confirm sample rate, channel
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

- **GitHub is a push mirror, not the working repo.** `origin` is the canonical repo; the GitHub copy is force-pushed from it on every push. Never push, merge a PR, or tag on GitHub: anything done there is overwritten at the next sync. To take a GitHub PR, fetch its branch, merge it into `origin`, and let the mirror carry it back. Tags pushed to `origin` reach GitHub through the mirror, which is what triggers the release workflow there.
- Update the version in Cargo.toml, then `git tag vX.Y.Z && git push --tags`.
- The release workflow builds Linux, macOS, and Windows binaries.
- Add a `## [X.Y.Z]` section to CHANGELOG.md for each version. The workflow uses it as the release notes.
- Whenever dependencies change, regenerate THIRD-PARTY-LICENSES.md with `cargo about generate --offline about.hbs -o THIRD-PARTY-LICENSES.md` (tool: `cargo install cargo-about --locked --features cli`; not a Cargo.toml dependency) and commit it.
- Submit every release to the registry (below), so the plugin store never skips a version.

### Registry submission

The plugin store lists releases from the registry, github.com/TrunkRecorder/plugins (see Publishing to the registry in writing-plugins.md).
A release gets there by a pull request from the user's fork of it. `scripts/registry-pr.sh` checks every step;
machine-specific details (where the registry clone is) are in CLAUDE.local.md.

1. Release first: the tag on `origin`, the mirror has carried it to GitHub, and the Release workflow has finished.
2. `scripts/registry-pr.sh prepare X.Y.Z --registry DIR`. It checks the release (tag on origin and GitHub at the same
   commit, Cargo.toml and CHANGELOG, the release published, the workflow passed, all four archives in SHA256SUMS with
   provenance attestations) and the clone (`origin` is the canonical repo, not GitHub; `upstream` is the registry;
   clean). Then it writes the entry with the registry's `add-release` on branch `icecast-endpoint-X.Y.Z`, runs the
   registry's `check`, checks that only `plugins/icecast-endpoint.json` and `index.json` changed and that the branch
   merges cleanly, commits, and prints the push command.
3. Push the branch to the clone's `origin` only, with the command it printed.
4. `scripts/registry-pr.sh verify X.Y.Z --registry DIR`. It checks the branch is on `origin`, that the mirror carried
   it to the GitHub fork at the same commit, the entry and `check` again, and whether a PR already exists. Only when
   every check passes does it print the steps (compare link, title, description) to hand to the user, who opens the
   PR on GitHub. Don't hand over PR steps that didn't come from a passing `verify`.

**Never push the branch straight to the GitHub fork.** The fork is a push mirror of the canonical repo, and each sync
deletes branches the canonical repo doesn't have. GitHub then closes the PR on its own, recorded as closed by the
user, because the mirror's token is theirs. If a registry PR closes itself, push its branch to `origin`, run
`verify` (it tells you to reopen the closed PR), and have the user press **Reopen pull request**.

## Working agreement

- Before implementing a feature, state which docs and events it depends on.
- Prefer small, tested increments. Run `cargo test` after each.
- Ask before adding dependencies, and before changing the settings schema once released.
