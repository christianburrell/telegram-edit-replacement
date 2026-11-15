# telegram-edit-replacement

A [Hermes Agent](https://hermes-agent.nousresearch.com/docs) plugin for Telegram. When the owner edits a
message they sent the bot, the edited text replaces the original instead of reaching the agent as a second,
repeated request. Out of the box, Hermes 0.22.2 reads an edit as a brand-new message with the full edited text
(same `message_id`, nothing marking it as an edit): every edit, even of an old message, starts or steers a turn
that looks like a repeat, edited slash commands run again, and edited photo or file captions are dropped.

This plugin relies on Hermes internals, not only on public plugin APIs, so a Hermes update can break it. It
protects itself with a version pin: on any Hermes other than the one it was verified on, it stays inert and
Hermes handles edits as it would without the plugin (see [Version pin](#version-pin)).

## Requirements

- Hermes Agent 0.22.2 (commit `d923a5eb48`), the only version tested, with python-telegram-bot 22.8 in Hermes'
  virtual environment.
- The Telegram platform, in direct messages with the bot. Group chats are not handled.
- Linux. `install.sh` needs bash and `python3`, and reads `/proc` to tell whether the gateway is running.
- `plugins.isolation: in_process` (the Hermes default): a plugin host refuses `register_telegram_handler`.

## Install

The plugin needs one setting: your own Telegram user id (the numeric id of the account you chat with the bot
from), in `plugins.entries.telegram-edit-replacement.settings.owner_user_id`. Without it the plugin loads
inactive and edits pass through as before.

With the Hermes CLI (Hermes takes a subdirectory after `owner/repo`):

```sh
hermes plugins install christianburrell/telegram-edit-replacement/plugin    # answer "Enable now?" with N
hermes config set plugins.entries.telegram-edit-replacement.settings.owner_user_id <your-telegram-user-id>
hermes plugins enable telegram-edit-replacement
```

The owner id is read when the plugin loads, so set it before enabling (or disable and enable again after
changing it).

From a clone:

```sh
git clone https://github.com/christianburrell/telegram-edit-replacement
cd telegram-edit-replacement
./install.sh <your-telegram-user-id>
```

`install.sh` (no restart needed):

- copies `plugin/` (code, manifest, pin) to `$HERMES_HOME/plugins/telegram-edit-replacement` (`HERMES_HOME`
  defaults to `~/.hermes`) and sets `plugins.entries.telegram-edit-replacement.settings.owner_user_id`;
- disables the plugin, has the running gateway reload its plugins (`scripts/reload_gateway_plugins.py`, run with
  Hermes' own Python: `$HERMES_PYTHON` if set, else the one venv named by `$HERMES_HOME/installs/*/facts.json`),
  then enables it. `hermes plugins enable` asks the gateway to reload only when the selection changes, and a
  gateway wires Telegram handlers only for plugins a reload adds, so this cycle makes first installs and updates
  take the same path. Without Hermes' Python, or when the reload fails, it prints a note and goes on: only
  Telegram handlers new in that version wait for the gateway's next restart;
- checks that the plugin is in `plugins.enabled` and not in `plugins.disabled`; if the enable failed it says how
  to recover (`hermes plugins enable telegram-edit-replacement`);
- waits up to 60 s (`EDIT_INSTALL_WAIT`) for the running gateway's load line in `$HERMES_HOME/logs/agent.log`:
  `telegram-edit-replacement loaded (pid <gateway pid>, active)`. It fails when the line says inactive or that a
  restart is needed, or when no line comes. With no running gateway it stops there: the gateway loads the plugin
  when it starts. Only lines written after the files and the owner setting are in place count; Hermes rotates
  `agent.log` by renaming it, so each log file is remembered by inode, size and first bytes. A `gateway.pid`
  whose recorded start time no longer matches the live process counts as no gateway.

Check: `grep -a telegram-edit-replacement ~/.hermes/logs/agent.log`. After every load it shows
`loaded (pid P, active)`; `native handlers wired (pid P)` and `caption handlers wired (pid P)` appear when a
Telegram client is first wired.

Rollback: `hermes plugins disable telegram-edit-replacement`. From the next edit on, edits behave as without the
plugin; no restart, and no chat history is touched. The inert handlers disappear at the next gateway restart. To
remove it, disable it, then delete `$HERMES_HOME/plugins/telegram-edit-replacement`.

## Settings

| Setting | Where | Default | Meaning |
|---|---|---|---|
| `owner_user_id` | `plugins.entries.telegram-edit-replacement.settings` in `config.yaml` | none (plugin inactive) | The Telegram user id whose edits are handled, digits only. Only this user's edits in their own DM with the bot are touched. |
| `EDIT_INSTALL_WAIT` | environment, `install.sh` only | `60` | Seconds `install.sh` waits for the gateway's load line. |
| `HERMES_PYTHON` | environment, `install.sh` and `scripts/hermes_python.py` | unset | Hermes' interpreter, when it can't be found from `$HERMES_HOME/installs/*/facts.json`. |

Fixed in the code: the earlier wording quoted in a notice is cut at 500 characters (`EARLIER_LIMIT`), 1000
characters of each message are kept (`STORE_LIMIT`) for the last 256 owner messages (`CACHE_ENTRIES`); the
recorders run in PTB handler groups 97 (text) and 98 (media). Hermes' own `display.busy_input_mode` (steer,
interrupt or queue) and `busy_text_mode` decide what happens to a notice that reaches a busy chat.

## How it works

Trigger: Telegram `edited_message` updates with text from `owner_user_id` in the owner's own DM (any topic), not
sent via an inline bot. Edited captions of the owner's photos, videos, audio files, voice messages and files
(documents, GIFs) are handled too. Everything else stays on Hermes' own handlers.

The edited message has not reached the agent yet: the correction goes with it, wherever Hermes keeps it: the
text batch (0.18-0.3 s after sending), the queue-mode debounce buffer, or the next-turn slot and the runner's
FIFO behind it.

- If it waits there alone and unchanged, the corrected text replaces it and the agent only ever gets the
  corrected text, with no notice.
- A corrected text that starts with `/`, or that Hermes would take as a command (including the plaintext admin
  phrases Hermes turns into `/restart`), never replaces the waiting text; it goes in a notice after it. Hermes'
  drain throws away queued text that starts with a known slash command, and a bare admin phrase without gateway
  control would reach the agent as a request.
- Otherwise (merged with other messages, with media, or a command) a notice is appended after it, so the agent
  gets both together, in order. When what waits starts with another message's command (a long command Hermes
  batched near the split limit), the notice goes on its own instead, since appended there it would become that
  command's arguments.

Hermes copied the message's text into another waiting event (a text merged into a queued photo, or a follow-up
merged into the next-turn slot): the plugin looks in the chat's next-turn slot. The slot must hold the message's
whole text as complete lines, appended after the message arrived, and must not be newer than the message. The
notice is appended there. Only messages the plugin saw waiting are searched for this way.

The edited message already reached the agent: a text batch pending for the chat is delivered first, then the
notice at once, on its own. The notice is fixed text: the corrected text replaces message N and is what the user
meant; ignore the earlier wording, even if it reaches you after this note; this is not a repeated or additional
request and the user's other messages still stand; act on the correction if it changes what is needed, otherwise
say so briefly; if you never saw the earlier version, treat it as new; say what was already done for the earlier
wording and ask before undoing it; it is conversation, not a slash command or an approval; anything after the end
line is a separate message. Then `Earlier wording (void): «...»` (or `Earlier wording: not available`),
`Corrected text:`, the edited text verbatim and the end line `[End of the corrected message.]`. When the corrected
text reads as a command, the notice adds that it was not run and asks the agent to have the user send it again.

Unchanged text (a formatting-only edit): nothing is delivered. "Unchanged" compares a SHA-256 of the user's whole
words as Hermes gives them to the agent (hidden links inlined, the bot's own @handle dropped); Hermes' note for a
reply to media names a newly cached file at every build, so it is not compared.

Caption edits: Hermes 0.22.2 drops an edited caption. The plugin turns it into a caption notice (same rules,
naming the media kind, saying the media did not change and is not attached again) and never downloads, attaches
or delivers the media again. Media that still waits (photo burst or album batch, next-turn slot, FIFO) gets the
notice appended to the event that carries it, found by identity, then by the path Hermes cached the media to.
Media the agent already has gets the notice on its own. A caption that reads as a command, or a carrier that
starts with another item's command, gets the notice on its own. A replaced file (another `file_unique_id`) is left
as Hermes leaves it: dropped. A waiting voice message that Hermes already transcribed has its cached transcript
dropped with every change, as Hermes' own merge does, so the drain transcribes it once more and echoes nothing
twice.

Busy chat: a notice sent on its own takes Hermes' busy lane like any message (steered, interrupting or queued,
per `display.busy_input_mode`). It is dispatched with the turn's busy-acknowledgement time set to now and the old
time put back right after, so Hermes does not answer the correction with its "steered into the current run"
bubble, while a real message is acknowledged as before.

Gateway control: text an edit made never has it (`allow_gateway_control=False`): it never runs a slash command and
never answers an approval, clarify or confirm prompt, including a waiting message replaced in place.

Topics: the edit gets Telegram's topic recovery before anything is looked up, as every text message does.

Memory: a recorder runs after Hermes queued each new owner DM message and keeps, for the last 256 messages, the
user's words (1000 characters), their length and fingerprint, a weak reference to the waiting event that carries
the message, and the events already waiting for the chat then; for media also the kind, the `file_unique_id` and
the cached path. Memory only, kept across plugin reloads, gone after a gateway restart.

Failure: an error before anything was queued or changed is logged (exception type and code location only, never
message text) and the update goes, once and untagged, to the Hermes handler that would have taken it. After that,
errors are only logged, so nothing is delivered twice.

Dispatch and cancellation mirror Telegram's own flush (`adapter._flush_buffered`): during teardown the pending
batch and then the edit are held for redispatch; a cancellation holds an event again unless Hermes marked it
stored.

Wiring that survives reloads: Hermes' forced plugin reload re-imports the module but never re-runs a Telegram
handler factory it already ran for a client. So two factories with fixed names (`wire_telegram`, `wire_captions`)
install thin handlers once per client (edited text and edited media in PTB group 0, ahead of Hermes' own
handlers; recorders in groups 97 and 98). They hold no logic: each update goes to the plugin generation loaded
last, found in process-wide state (`sys.modules["_hermes_telegram_edit_replacement_state"]`, kept per Hermes
home). There is no unload hook, so edits are never handed to Hermes untagged between the unload and the new load.
`hermes plugins disable` only edits `config.yaml`, so the plugin re-reads `plugins.enabled` / `plugins.disabled`
(with Hermes' own YAML loader, only when the file changed) and a disable takes effect for the next edit; with its
own `plugin.yaml` gone it reads as disabled. The thin handlers stay as first wired; the plugin fingerprints their
source and warns (and says so in its load line) when a newer version would need a gateway restart to change them.

## What it never does

- Rewrite or delete stored history, the earlier message or the reply to it. Only events that have not reached the
  agent yet are changed.
- Cancel, roll back or redo work by itself.
- Touch fresh messages, other users, other chats, groups, bots, channel posts or live-location updates.
- Download, attach or deliver media again for a caption edit.
- Use the network, credentials or a database, or write to disk; nothing is persisted. It reads its pin, its
  manifest, `config.yaml` and the pinned Hermes files. The Hermes functions it calls do what they do for any
  message (building an edit's turn caches the media a reply points to; topic recovery reads Hermes' session
  store).

## Limits and known issues

- An edited message already inside a running turn, or held by the adapter during a reconnect, is not changed where
  it is: the notice goes on its own and tells the agent to ignore the earlier wording if it reaches it later.
- The earlier wording is unknown for messages sent before the gateway last started; the notice says so.
- The search for a copied message matches by content (complete lines with the user's whole words appended to the
  slot after the message arrived). It can take the wrong event when the same words were sent twice, when an older
  event with the same text reached the slot late, or when another correction notice quotes the same words as a
  whole line. The notice then waits in the slot for the next turn instead of reaching the running turn.
- A real message that Hermes handles while suspended inside its busy handler for the plugin's notice also gets no
  busy acknowledgement for that moment; it is still steered or queued as usual.
- A cancel that lands right after Hermes steered, redirected or merged the plugin's own notice holds it again, and
  the reconnect delivers it a second time. Hermes' own flush does the same for any message.
- After an in-place replacement the waiting message has no gateway control: a photo sent right after it gets its
  own turn, and an answer to an approval or clarify prompt edited while it still waits in its text batch answers
  nothing (the prompt waits for its timeout).
- Replies to media: every edit makes Hermes cache the replied-to media again (also a formatting-only edit), and a
  notice appended to a waiting reply ends with Hermes' note naming a second cached copy that is not attached.
- A command that Hermes' runner merged into a queued photo is not followed; its edit goes as a separate notice.
- Caption edits:
  - a replaced photo or file is dropped, as Hermes drops it; without a record (sent before the gateway started) a
    swap can't be told from a caption edit;
  - media the plugin never saw waiting gets its notice on its own; in steer mode that notice can reach the running
    turn before the media's own turn (it says to ignore the earlier caption if it comes later);
  - a burst or album in flight (popped by Hermes' flush, not yet queued) is in no waiting place for that moment;
  - a photo whose download failed, sent while its chat's photo burst holds exactly one path no record knows, takes
    that path; such paths come only from media sent before the caption handlers were wired or records evicted from
    the 256 kept;
  - a correction appended to a waiting voice message makes Hermes transcribe it once more; if that call fails, the
    turn gets Hermes' "could not be transcribed" note with the caption and the notice;
  - a correction written to a waiting voice message while Hermes is still transcribing that same message (the
    interrupt monitor, interrupt mode) can be lost, as Hermes' own merges into the slot can;
  - with `busy_text_mode: queue`, a caption notice sent on its own can land in a slot whose text starts with a
    known command, and Hermes' drain then drops the slot, notice included (as it drops that media without the
    plugin);
  - stickers, video notes and locations have no caption.
- Edits in chats that Hermes routes to another profile pass through untagged.
- The enabled check reads the home's `config.yaml` only: Hermes' managed-scope overlay and `${VAR}` expansion are
  not applied.
- Hermes answers every inbound turn, so an edit sent on its own always gets a reply (one line for a typo fix).
- During the few seconds of `install.sh`'s disable, reload and enable, edits pass through untagged.
- `install.sh` checks the load only for a gateway started from the plugin's own Hermes home; for a gateway that
  serves this profile from another home (multiplexed profiles) it says where to look and exits 0.

## Version pin

`plugin/verified_hermes.json` pins SHA-256 digests of the 17 Hermes files the delivery rules rely on
(`scripts/pin_hermes.py` `RELIED_ON`), the Hermes commit and the python-telegram-bot version. At every load the
plugin checks:

- the digests of those files in the Hermes checkout it runs from (the loaded `hermes_cli` package's parent);
- that the process started from the pinned commit (`hermes_cli/version_info.py` `get_code_identity`): a gateway
  keeps running the code it imported at startup, so files updated on disk since it started are not what it runs;
- the loaded `telegram.__version__` (a venv sync can change the library without touching any Hermes file).

Any difference, or a pin that is missing, unreadable or incomplete, keeps the plugin inert: it logs what differs,
its load line says `inactive: Hermes differs from the version this plugin was verified on`, and Hermes handles
edits as without the plugin. A changed Hermes could, for example, commit busy follow-ups to a durable queue at
arrival, where changing a waiting event in memory would silently lose a correction.

To re-pin after a Hermes update, from a clone:

```sh
python3 scripts/hermes_python.py -B scripts/pin_hermes.py
./install.sh <your-telegram-user-id>
```

`pin_hermes.py` pins `~/.hermes/hermes-agent` (the checkout the suites test; there is no option to pin another
tree). It runs both test suites against it first and writes the pin only when they pass, the pipeline tests ran,
and neither the commit nor a pinned file or library changed while they ran. It refuses (non-zero exit, nothing
written) while any relied-on file differs from the commit, or is missing, untracked or ignored. A failing suite
means the plugin needs work before it runs on that Hermes. A load line naming a `running commit` means the gateway
still runs the code it started with; it loads the plugin active after its next restart.

## Hermes internals it relies on (0.22.2, `d923a5eb48`)

- `hermes_cli/plugins.py:829-850` `register_platform_handler` / `register_telegram_handler`; `:393-399`
  `profile_name`; `:267` `get_config` (`plugins.entries.<key>.settings`); `:1294-1327` forced reloads.
- `hermes_cli/plugins_loader.py:229-237` (Telegram handlers re-wired only for plugins a reload adds);
  `gateway/run_plugin_rewire.py:44-49, 81-117`; `hermes_cli/plugins_cmd.py:753-766, 1024-1026` (disable only edits
  config.yaml and sends no reload).
- `hermes_constants.py:111` `get_hermes_home`, `:129` `hermes_home_key`; `utils.fast_safe_load`;
  `hermes_cli/config.py:2220-2258, 2356-2359` (last good config).
- `gateway/platforms/base.py:2304-2345` `_wire_plugin_handlers` / `rewire_plugin_handlers`;
  `plugins/platforms/telegram/adapter.py:2884-2923` core handlers (text handler group 0, commands, group 99) and the
  hoist of late plugin handlers ahead of the first core handler of their group.
- `adapter.py`: `6457-6464` `_build_triggered_event`; `6466-6497` `_handle_text_message` / `_handle_command` (the
  fallbacks; a command near the split limit is batched, `6491-6496`); `6534-6537` `_text_batch_key`; `6539-6545`
  `_enqueue_text_event`; `6547-6576` `_flush_buffered`; `785-788` `_should_drop_delayed_delivery`; `6114-6119`
  `_clean_bot_trigger_text`; `7067-7105` `_build_message_event`; `6210` `_media_message_type`; `6602-6645` photo
  bursts (`_photo_batch_key`, `_route_photo_event`, `_merge_into_pending`); `6714-6729` and `188` image documents
  (`_TELEGRAM_IMAGE_EXTENSIONS`); `6779-6835` `_handle_media_message`; `6837-6852` albums (`_media_group_events`).
- `telegram_context.py:32-37` `group_trigger_text`; `telegram_entities.expand_link_entities`;
  `telegram_held_inbound.py:38` `_hold_inbound_event`; `update_admission.py:129-168` (updates are FIFO per chat, so
  the recorders run after Hermes queued the fresh message).
- `gateway/platforms/base.py`: `2566-2610` text batching (`_pending_text_batches`, `_pop_text_batch`,
  `_dispatch_text_batch`; `_append_text` `1640-1642`); `2373-2390` `_apply_topic_recovery`; `4035-4075`
  `handle_message` and `_event_session_key`; `1662-1680` `coerce_plaintext_gateway_command`; `4077-4185` busy
  routing; `3811-3907` queue-mode debounce (`_text_debounce_store`); `1800-1845` `merge_pending_message_event`
  (`1834-1838`: a merge drops the transcript cache, keeps the echo ledger); `3798-3806` `_merge_caption`.
- `gateway/run_busy.py`: `100-114` `_overflow_queue` / `_enqueue_fifo` (sets `_gateway_accepted`); `375-424`
  `_queue_or_replace_pending_event`; `427-440, 600-635, 691-700, 845-870` (steer, interrupt and queue, voice
  transcription); `882-905` the busy acknowledgement and the turn's `busy_ack_ts`.
- `gateway/run_adapters.py:1878-1884` `adapter.gateway_runner`; `1347-1365` `_wire_adapter_handlers`;
  `1745-1760` the runner's session key.
- `gateway/run_inbound.py:583-590` `_hm_merge_pending_for_source`, `641`, `714` (busy merges); `1138-1166` (unknown
  commands); `1233-1250` `_hm_dispatch_idle_commands`; `1718-1719`, `1996-2023` (the transcript cached on a waiting
  event, `_transcribe_pending_audio_event_once`).
- `gateway/run_turn.py:3627-3730` `_run_agent_drain_pending` (drops queued slash-command text, `3712-3724`);
  `3296-3315` the interrupt monitor.
- `gateway/platforms/event.py:97-135` `allow_gateway_control`, `get_command()` (rejects a name holding `/`).
- `hermes_cli/version_info.py:268-332` `get_code_identity`; `gateway/status.py:792-807` (`code_sha`).
- `install.sh` reads `gateway/status.py:782-785` `_build_pid_record` (`start_time` from `/proc/<pid>/stat` field 22,
  `470-504`) and the log rotation in `hermes_logging.py:362-381` (`agent.log`, 5 MB, 3 backups, by renaming).
- `gateway/control_socket.reload_gateway_plugins` and `hermes_constants.get_default_hermes_root`
  (`scripts/reload_gateway_plugins.py`).
- Not a `pre_gateway_dispatch` hook: the busy path (`base.py:4071-4072` → `_handle_message_while_active` →
  `run_busy.py:807`) never calls it.

`hermes plugins validate plugin` flags the private imports (catalog rule 9): `hermes_cli.version_info.get_code_identity`,
`utils.fast_safe_load`, `telegram_context.group_trigger_text`, `telegram_entities.expand_link_entities` and
`adapter._TELEGRAM_IMAGE_EXTENSIONS`, and notes that `register_telegram_handler` is in-process only.

## Tests

The integration tests need Hermes' own interpreter (it has python-telegram-bot and Hermes' packages).
`scripts/hermes_python.py` finds it: `$HERMES_PYTHON` if set, else the one virtual environment named by
`$HERMES_HOME/installs/*/facts.json` (`HERMES_HOME` defaults to `~/.hermes`); it stops with a message when it
finds none. Run from the repository root:

```sh
# unit tests and install.sh (any Python 3; a stub `hermes` on PATH, never the real CLI): 89 tests
python3 -B -W error::ResourceWarning -m unittest tests.test_replacement tests.test_install

# everything, with Hermes' interpreter: 93 tests, one of which runs scripts/offline_pipeline.py (153 tests)
python3 scripts/hermes_python.py -B -W error::ResourceWarning -m unittest discover -s tests

# the mutation catalog's patterns (339): each matches exactly once and each mutant still parses
python3 scripts/hermes_python.py -B scripts/check_mutations.py --check
```

- `tests/test_replacement.py`: the policy, notices, wiring, config gate, pin check, and `pin_hermes.py` with
  stand-in suites on a throwaway git checkout.
- `tests/test_install.py`: `install.sh` against a stub `hermes` and a stand-in for Hermes' Python, with `HOME` and
  `HERMES_HOME` in a temp dir.
- `tests/test_reload.py`: `scripts/reload_gateway_plugins.py` against Hermes' real control socket server in a
  throwaway home (never a live gateway).
- `tests/test_hermes.py` runs `scripts/offline_pipeline.py` in a child process: the plugin loaded by Hermes' own
  plugin loader into a throwaway `HERMES_HOME`; real PTB dispatch through the shipped Telegram adapter (no
  network); text batching; the busy lane with Hermes' real queue-mode debounce, next-turn queue, slot merge and
  drain; teardown holds, cancellation and redispatch; topic recovery; forced reloads through the real
  `reload-plugins` verb and late wiring through the runner's real re-wire subscription; disable, a removed
  directory and an unverified Hermes; the command reading compared with the runner's real idle dispatch; caption
  edits through Hermes' real media handler (only Telegram's file download and Hermes' media cache are stubbed);
  speech-to-text of queued voice messages through the runner's real methods (only the provider call and
  Telegram's send are stood in for). Not covered: Hermes' own steer entry with a live agent; a stand-in runner
  steers the way that entry ends. The child is hermetic: `HERMES_DISABLE_LAZY_INSTALLS=1`, gateway locks in the
  temp dir, and plugin selection written to the temp `config.yaml` instead of calling `hermes plugins
  enable/disable`.
- `scripts/check_mutations.py --workers 2` (with Hermes' interpreter) runs the whole catalog: every guard of the
  plugin, `install.sh` and the reload script broken once, each mutant must make its suite fail. It takes a long
  time; `--only <names>` re-checks single mutants.

## License

MIT, see [LICENSE](LICENSE).
