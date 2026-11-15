"""telegram-edit-replacement: a Telegram edit replaces the message it edits.

The rule: when the owner edits a message, the earlier wording is ignored and the edited text is
what they meant; it must never look like the same message sent twice.

Wiring. A native PTB handler factory with a fixed name (``ctx.register_telegram_handler``) installs
two thin handlers once per Telegram client: the owner's edited DM text in group 0, ahead of Hermes'
text handler, and a recorder of the owner's new messages in group 97. Both hand each update to the
plugin code loaded last, found in process-wide state. Hermes re-imports this module on every forced
plugin reload and never re-runs a factory it already ran for a client, so the handlers stay wired
while each reload swaps the code behind them. `hermes plugins disable` (config.yaml, read on use)
hands edits straight back to Hermes' own handlers.

Delivery. While the edited message still waits to reach the agent (text batch, queue-mode debounce,
next-turn slot or the runner's FIFO, also when Hermes copied its text into another waiting event),
the correction goes with it: the corrected text replaces it when it waits alone and unchanged and
does not read as a command, otherwise a replacement notice is appended to what waits. Once it has
reached the agent, anything pending for the chat goes first and the notice follows at once. The notice
quotes the earlier wording (when known) and the corrected text, and ends with an end line. Text made
by an edit never gets gateway control (no slash command, approval or clarify answer). Nothing stored
is rewritten.

Captions. Edited captions of the owner's media (which Hermes drops) go through their own handlers, wired by
a second fixed-name factory so the text handlers stay as first wired: a notice appended to the event that
still carries the media (found by identity, then by the path Hermes cached the media to), or sent on its own
once the agent has it. The media is never downloaded or delivered again; declining delivers nothing.

Stepping aside. The plugin leaves every edit to Hermes when Hermes' relied-on source or its Telegram
library differs from the version it was verified on (verified_hermes.json, written by
scripts/pin_hermes.py after the tests pass), or the gateway still runs another Hermes commit than the
pinned one; when config.yaml disables it; or once its directory is gone.
Spec and Hermes call sites: README.md at the repository root.
"""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
import threading
import time
import traceback
import types
import weakref

log = logging.getLogger("hermes.plugins.telegram_edit_replacement")

PLUGIN_KEY = "telegram-edit-replacement"
EARLIER_LIMIT = 500    # characters of the earlier wording quoted in the notice
STORE_LIMIT = 1000     # characters of a message's wording kept in memory for that quote
CACHE_ENTRIES = 256    # recent owner messages remembered (memory only, lost on restart)
RECORDER_GROUP = 97    # PTB group of the recorder; core handlers use 0 and 99
MEDIA_RECORDER_GROUP = 98   # PTB group of the media recorder (caption edits); the text recorder keeps 97
END_LINE = "[End of the corrected message.]"
PIN_FILE = "verified_hermes.json"   # the Hermes source this plugin was verified on (scripts/pin_hermes.py)
PINNED_PACKAGES = ("python-telegram-bot",)   # libraries pinned with it: a venv sync can change them alone
_LOADED_AS = {"python-telegram-bot": "telegram"}   # each pinned library's top-level module
# Hermes evicts hermes_plugins.<name>* on every reload; an entry outside that namespace survives.
_STATE_MODULE = "_hermes_telegram_edit_replacement_state"


def numeric_id(value):
    if not isinstance(value, (str, int)):   # bool is an int, but str(True) is not digits
        return None
    text = str(value)
    return str(int(text)) if re.fullmatch(r"[0-9]{1,20}", text) else None


def fingerprint(text):
    """Digest of a whole text: decides 'unchanged' however long the text is."""
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def _as_set(value):
    if not isinstance(value, list):
        return None
    try:
        return set(value)
    except TypeError:
        return None


def plugin_enabled(config, key=PLUGIN_KEY):
    """Hermes' rule (plugins_discovery.gate_manifest over _get_enabled/_get_disabled_plugins):
    plugins.disabled wins, then the key must be in plugins.enabled. A value that isn't a list, or
    can't be read as a set, counts as nothing disabled / nothing enabled."""
    plugins = config.get("plugins") if isinstance(config, dict) else None
    if not isinstance(plugins, dict):
        return False
    disabled = _as_set(plugins.get("disabled")) or set()
    enabled = _as_set(plugins.get("enabled")) or set()
    return key not in disabled and key in enabled


def hermes_source_root():
    """The Hermes checkout this process runs: the parent of the loaded hermes_cli package."""
    origin = getattr(sys.modules.get("hermes_cli"), "__file__", None)
    if not origin:
        try:
            import importlib.util
            origin = getattr(importlib.util.find_spec("hermes_cli"), "origin", None)
        except (ImportError, ValueError):
            origin = None
    return Path(origin).resolve().parent.parent if origin else None


def _installed_version(name):
    """The version of a pinned library this process runs: its loaded module's own __version__ (a venv
    sync replaces the files under a running gateway, not the module it imported), else the installed
    distribution's, which an import would load."""
    loaded = getattr(sys.modules.get(_LOADED_AS.get(name, "")), "__version__", None)
    if isinstance(loaded, str) and loaded:
        return loaded
    try:
        from importlib import metadata
        return metadata.version(name)
    except Exception:
        return None


def _running_commit():
    """The Hermes commit this process started from: Hermes' own code identity, worked out once per
    process (hermes_cli/version_info.py; the gateway stamps it into gateway_state.json as code_sha). A
    gateway keeps running the code it imported at startup, whatever is on disk later. None if unknown."""
    try:
        from hermes_cli.version_info import get_code_identity
        sha = get_code_identity().get("sha")
    except Exception:
        return None
    return sha if isinstance(sha, str) and sha else None


def pin_mismatches(pin_path, root, version_of=None, running_commit=None):
    """The relied-on Hermes files whose content differs from the version this plugin was verified on,
    each pinned library whose version in this process differs, and the Hermes commit this process
    started from when it is not the pinned one (files pinned on disk after a gateway started are not
    what it runs). Every pinned file when Hermes can't be found; the pin itself when it is missing,
    unreadable or incomplete. Empty means verified."""
    version_of = version_of or _installed_version
    running_commit = running_commit or _running_commit
    try:
        record = json.loads(Path(pin_path).read_text(encoding="utf-8"))
        pinned, packages, commit = record["files"], record["packages"], record["hermes_commit"]
    except Exception:
        return [PIN_FILE]
    if not isinstance(pinned, dict) or not pinned:
        return [PIN_FILE]
    if not isinstance(packages, dict) or not all(
            isinstance(packages.get(name), str) and packages.get(name) for name in PINNED_PACKAGES):
        return [PIN_FILE]
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
        return [PIN_FILE]
    changed = []
    for rel, digest in sorted(pinned.items()):
        path = Path(rel)
        actual = None
        if root is not None and not path.is_absolute() and ".." not in path.parts:
            try:
                actual = hashlib.sha256((Path(root) / path).read_bytes()).hexdigest()
            except OSError:
                actual = None
        if actual is None or actual != digest:
            changed.append(rel)
    for name in PINNED_PACKAGES:
        installed = version_of(name)
        if installed != packages[name]:
            changed.append(f"{name} {installed}" if installed else f"{name} (not installed)")
    running = running_commit()
    if running != commit:
        changed.append(f"running commit {running or 'unknown'}")
    return changed


def _hermes_home():
    """The Hermes home this load runs in: register() runs inside the profile's home scope."""
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home())
    except Exception:
        return Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")


def state_key(home=None):
    """State is kept per Hermes home, not per profile name: Hermes names every non-standard home
    'custom' (hermes_cli/profiles.py), so two homes in one process could share it."""
    home = Path(home) if home is not None else _hermes_home()
    try:
        from hermes_constants import hermes_home_key
        return hermes_home_key(home)
    except Exception:
        return str(home.expanduser().resolve(strict=False))


def shared_state(key):
    """One Hermes home's process-wide state: the generation loaded last, what the plugin knows of the
    owner's recent messages and the wired Telegram clients. It outlives every reload of this module."""
    holder = sys.modules.get(_STATE_MODULE)
    if holder is None:
        fresh = types.ModuleType(_STATE_MODULE)
        fresh.__dict__.update(lock=threading.Lock(), profiles={})
        holder = sys.modules.setdefault(_STATE_MODULE, fresh)
    with holder.lock:
        state = holder.profiles.get(key)
        if state is None:
            state = holder.profiles[key] = types.SimpleNamespace(
                lock=threading.Lock(), current=None, wording={}, wired=weakref.WeakKeyDictionary())
        return state


def register(ctx):
    home = _hermes_home()
    here = Path(__file__).resolve().parent
    state = shared_state(state_key(home))
    policy = ReplacementPolicy(ctx.get_config("owner_user_id"), ctx.profile_name)
    changed = pin_mismatches(here / PIN_FILE, hermes_source_root())
    gate = ConfigGate(home / "config.yaml", manifest=here / "plugin.yaml")
    generation = Generation(state, policy, gate, verified=not changed)
    ctx.register_telegram_handler(make_factory(state))
    ctx.register_telegram_handler(make_caption_factory(state))
    # No unload hook: Hermes unloads every plugin at the start of a forced reload and loads the new
    # code a moment later, and switching off in between would hand edits to Hermes untagged. This
    # generation serves until a newer load replaces it; config.yaml (ConfigGate) turns it off.
    with state.lock:
        state.current = generation      # last: a load that failed above never takes over
    problems = []
    if changed:
        problems.append("Hermes differs from the version this plugin was verified on")
        log.warning("telegram-edit-replacement: Hermes differs from the version it was verified on (%s); "
                    "edits pass to Hermes untagged until it is verified again (scripts/pin_hermes.py)",
                    ", ".join(changed))
    if policy.owner is None:
        problems.append("no owner_user_id")
        log.warning("telegram-edit-replacement: owner_user_id is missing or not a Telegram user id; "
                    "edits pass through untagged")
    status = "inactive: " + "; ".join(problems) if problems else "active"
    if (any(digest != WIRING_DIGEST for digest in list(state.wired.values()))
            or any(digest != CAPTION_WIRING_DIGEST for digest in list(_caption_wired(state).values()))):
        status += "; restart the gateway to update its Telegram handlers"
        log.warning("telegram-edit-replacement: its Telegram handlers changed in this version; they stay as "
                    "first wired until the gateway restarts (hermes gateway restart)")
    log.info("telegram-edit-replacement loaded (pid %s, %s)", os.getpid(), status)


def make_factory(state):
    """The Telegram handler factory. Hermes runs each (plugin, qualname) once per Telegram client and
    keeps that record across reloads, so this name never changes and the factory holds no generation."""
    def wire_telegram(native, adapter):
        _wire(state, native, adapter)
    return wire_telegram


def wiring_digest():
    """Fingerprint of what the wired handlers keep running (their code, the fallback and logging they
    call, the recorder's PTB group): Hermes never wires a client twice, so a change to it takes effect
    only after a gateway restart. Taken once, as the module loads (WIRING_DIGEST): by the time an older
    load wires a new client, the file may already hold a newer version. None when it can't be read."""
    import inspect
    try:
        source = "".join(inspect.getsource(f) for f in (_wire, deliver_like_core, _log_failure))
        return fingerprint(f"{source}\nRECORDER_GROUP={RECORDER_GROUP!r}")
    except Exception:
        return None


def _log_failure(what, exc):
    """Exception type and code location only: exception messages can quote the user's text."""
    frames = traceback.extract_tb(exc.__traceback__)
    where = f"{Path(frames[-1].filename).name}:{frames[-1].lineno}" if frames else "?"
    log.warning("telegram-edit-replacement: %s (%s at %s)", what, type(exc).__name__, where)


async def deliver_like_core(adapter, update, context):
    """Hand an update this plugin took back to the Hermes handler that would have taken it."""
    from telegram.ext import filters
    if filters.COMMAND.check_update(update):
        await adapter._handle_command(update, context)
    else:
        await adapter._handle_text_message(update, context)


def _wire(state, native, adapter):
    """Install the two handlers on one Telegram client, once. They outlive reloads of this module, so
    they only delegate: to the generation loaded last, or back to Hermes."""
    if native in state.wired:
        return
    from telegram.ext import MessageHandler, filters

    class Live(filters.MessageFilter):
        def filter(self, message):
            generation = state.current
            if generation is None:
                return False
            try:
                return bool(generation.accepts_message(message))
            except Exception as exc:
                _log_failure("filter failed; Hermes handles the update", exc)
                return False

    async def edited(update, context):
        try:   # False, or an error before anything was queued: Hermes' own handler takes it, once
            handled = await state.current.handle_edit(update, context, adapter)
        except Exception as exc:
            _log_failure("could not handle an edit; delivered untagged", exc)
            handled = False
        if not handled:
            await deliver_like_core(adapter, update, context)

    async def record(update, context):
        generation = state.current
        if generation is None:
            return
        try:
            generation.record(update, adapter)
        except Exception as exc:
            _log_failure("could not record a message", exc)

    live = Live(name="telegram-edit-replacement")
    native.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & filters.TEXT & live, edited), group=0)
    native.add_handler(MessageHandler(filters.UpdateType.MESSAGE & filters.TEXT & live, record),
                       group=RECORDER_GROUP)
    state.wired[native] = WIRING_DIGEST
    log.info("telegram-edit-replacement native handlers wired (pid %s)", os.getpid())


def make_caption_factory(state):
    """The caption handlers' own factory, with its own fixed name: Hermes wires it once per Telegram client
    (also into a running gateway, when an install's disable/enable reload loads the plugin anew), and the
    text handlers' wiring above stays exactly as first wired."""
    def wire_captions(native, adapter):
        _wire_captions(state, native, adapter)
    return wire_captions


def caption_wiring_digest():
    """wiring_digest() for the caption handlers (taken once, as the module loads: CAPTION_WIRING_DIGEST)."""
    import inspect
    try:
        source = "".join(inspect.getsource(f) for f in (_wire_captions, _log_failure))
        return fingerprint(f"{source}\nMEDIA_RECORDER_GROUP={MEDIA_RECORDER_GROUP!r}")
    except Exception:
        return None


def _caption_wired(state):
    """The Telegram clients the caption handlers are wired on (added to a state an older version made)."""
    with state.lock:
        wired = getattr(state, "caption_wired", None)
        if wired is None:
            wired = state.caption_wired = weakref.WeakKeyDictionary()
        return wired


def _wire_captions(state, native, adapter):
    """Install the caption handlers on one Telegram client, once: the owner's edited media in group 0, ahead of
    Hermes' media handler (which matches an edited update and drops it), and a recorder of the owner's new
    media messages after Hermes queued them. Both delegate to the generation loaded last. Hermes drops edited
    media, so nothing is ever handed back to it: declining or failing delivers nothing, as before."""
    wired = _caption_wired(state)
    if native in wired:
        return
    from telegram.ext import MessageHandler, filters

    class CaptionLive(filters.MessageFilter):
        def filter(self, message):
            generation = state.current
            if not callable(getattr(generation, "handle_caption_edit", None)):
                return False        # nothing loaded, or a version without caption edits: Hermes' handlers
            try:
                return bool(generation.accepts_message(message))
            except Exception as exc:
                _log_failure("caption filter failed; Hermes handles the update", exc)
                return False

    async def edited(update, context):
        handle = getattr(state.current, "handle_caption_edit", None)
        if not callable(handle):
            return
        try:
            await handle(update, context, adapter)
        except Exception as exc:
            _log_failure("could not handle a caption edit; nothing delivered", exc)

    async def record(update, context):
        recorder = getattr(state.current, "record_media", None)
        if not callable(recorder):
            return
        try:
            recorder(update, adapter)
        except Exception as exc:
            _log_failure("could not record a media message", exc)

    live = CaptionLive(name="telegram-edit-replacement-captions")
    media = filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.Document.ALL
    native.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & media & live, edited), group=0)
    native.add_handler(MessageHandler(filters.UpdateType.MESSAGE & media & live, record),
                       group=MEDIA_RECORDER_GROUP)
    wired[native] = CAPTION_WIRING_DIGEST
    log.info("telegram-edit-replacement caption handlers wired (pid %s)", os.getpid())


class Seen:
    """What the plugin knows of one owner message: the user's words as the agent gets them, without the
    notes Hermes adds around them (quoted in a notice), the fingerprint and length of those whole
    words (for 'unchanged', and to find them where Hermes copied
    it), and the pending event that carries it while it waits, weakly held. holder_digest is set when
    that event carried only this message: the fingerprint of its text then. marks: the events that
    already waited for the chat's next turns at that moment, with the length of their text then."""
    __slots__ = ("wording", "fingerprint", "holder", "holder_digest", "length", "marks")

    def __init__(self, wording, fingerprint, holder=None, holder_digest=None, length=None, marks=()):
        self.wording = wording
        self.fingerprint = fingerprint
        self.holder = holder
        self.holder_digest = holder_digest
        self.length = length
        self.marks = marks          # what already waited for the chat then: _waiting_marks

    def carrier(self):
        return self.holder() if self.holder is not None else None


class SeenMedia(Seen):
    """What the plugin knows of one owner media message: Seen for its caption (the wording may be empty: no
    caption), plus media = (kind, file_unique_id, cached path): what the notice calls it, the file Telegram
    sent (a different one means the media itself was replaced) and where Hermes cached it, which stays in
    whichever waiting event Hermes merges it into (merges copy media_urls)."""
    __slots__ = ("media",)

    def __init__(self, *args, media=(None, None, None), **kwargs):
        super().__init__(*args, **kwargs)
        self.media = media


class WordingCache:
    """What the plugin knows of the owner's recent messages, keyed by (chat, message). The store is
    passed in so it outlives a reload; memory only, gone on restart."""

    def __init__(self, store=None, max_entries=CACHE_ENTRIES):
        self.max_entries = max_entries
        self._items = store if store is not None else {}

    def put(self, chat_id, message_id, seen):
        key = (str(chat_id), str(message_id))
        self._items.pop(key, None)
        if not isinstance(seen.wording, str) or not (seen.wording or getattr(seen, "media", None)):
            return
        seen.wording = seen.wording[:STORE_LIMIT]
        self._items[key] = seen
        while len(self._items) > self.max_entries:
            self._items.pop(next(iter(self._items)))

    def get(self, chat_id, message_id):
        return self._items.get((str(chat_id), str(message_id)))

    def records(self):
        return list(self._items.values())


class ConfigGate:
    """Is the plugin enabled in this profile's config.yaml right now? `hermes plugins disable` only
    edits that file and the running gateway keeps loaded plugins, so the file is read on use (again
    only after it changed), with Hermes' own YAML loader. Until a read succeeds the gate counts as
    enabled: Hermes loaded this code, so its config, or its last good config when the file does not
    parse (config._last_known_good_fallback), enabled the plugin. A file that stops parsing later
    keeps the last state read, as Hermes keeps its last good config. With the plugin's own manifest
    gone (its directory removed) it reads as disabled: Hermes would not load it again."""

    def __init__(self, path, key=PLUGIN_KEY, manifest=None):
        self.path = Path(path)
        self.key = key
        self.manifest = Path(manifest) if manifest is not None else None
        self._signature = None
        self._enabled = True

    def enabled(self):
        if self.manifest is not None and not self.manifest.exists():
            return False
        try:
            st = os.stat(self.path)
        except OSError:
            return False
        signature = (st.st_ino, st.st_mtime_ns, st.st_size)
        if signature != self._signature:
            try:
                from utils import fast_safe_load
                with open(self.path, encoding="utf-8-sig") as fh:
                    self._enabled = plugin_enabled(fast_safe_load(fh), self.key)
            except Exception as exc:
                _log_failure("could not read config.yaml; keeping the last state read", exc)
            self._signature = signature
        return self._enabled


def _tearing_down(adapter):
    check = getattr(adapter, "_should_drop_delayed_delivery", None)
    return bool(check()) if callable(check) else False


# What Hermes derives from a waiting event's text and reads instead of it: a queued voice note's speech-to-text
# result (run_inbound.py _transcribe_pending_audio_event_once). Hermes drops it whenever it changes a waiting
# event and keeps the echo ledger (_gateway_pending_stt_echoed), so a transcript is never echoed twice
# (base.py merge_pending_message_event).
_DERIVED_FROM_TEXT = ("_gateway_pending_stt_text", "_gateway_pending_stt_transcripts")


def _set_waiting_text(event, text):
    """Change a waiting event's text the way Hermes' own merge does: what Hermes derived from the earlier text
    goes with it, and it derives it again from this text when the event's turn comes."""
    event.text = text
    for name in _DERIVED_FROM_TEXT:
        if hasattr(event, name):
            delattr(event, name)


def _carried_text(adapter, message):
    """The text Hermes gives the agent for a message (adapter._build_triggered_event, without its
    replied-media cache): hidden links inlined, the bot's own @handle dropped. What a formatting-only
    edit leaves unchanged."""
    from plugins.platforms.telegram.telegram_context import group_trigger_text
    from plugins.platforms.telegram.telegram_entities import expand_link_entities
    return group_trigger_text(adapter, message, expand_link_entities(message))


def _waiting(adapter, event, carrier):
    """Is this pending event still on its way to the agent? Until then Hermes keeps the same event
    object in its text batch, the queue-mode debounce buffer, the session's next-turn slot or the
    runner's FIFO behind that slot."""
    def holds(events):
        return any(candidate is carrier for candidate in events)

    if holds(adapter._pending_text_batches.values()):
        return True
    debounce = getattr(adapter, "_text_debounce_store", None)
    if callable(debounce) and holds(getattr(state, "event", None) for state in debounce().values()):
        return True
    if holds((getattr(adapter, "_pending_messages", None) or {}).values()):
        return True
    overflow = getattr(getattr(adapter, "gateway_runner", None), "_overflow_queue", None)
    return callable(overflow) and holds(overflow(adapter._event_session_key(event)) or ())


async def _dispatch_now(adapter, key):
    """Dispatch the text batch for key now, the way Telegram's own flush does (adapter._flush_buffered):
    held when teardown has started, and held again when cancelled before Hermes took it."""
    event = adapter._pop_text_batch(key)
    if event is None:
        return
    if _tearing_down(adapter):
        adapter._hold_inbound_event(event, where="edit-replacement-flush")
        return
    try:
        await adapter._dispatch_text_batch(event)
    except asyncio.CancelledError:
        if getattr(event, "_gateway_accepted", None) is not True:     # Hermes has not stored it
            adapter._hold_inbound_event(event, where="edit-replacement-flush-cancelled")
        raise


def _hush_busy_ack(adapter, event):
    """Run right before a correction notice is dispatched. Hermes answers a message that reaches a busy chat with
    its busy acknowledgement ("⏩ Steered into the current run ...", or queued, or interrupting), at most once per
    30 s of a turn: the turn's busy_ack_ts (gateway/run_busy.py:888-896), checked inline while the dispatch is
    awaited. A correction is no new message, so that time is set to now on the session state Hermes' busy handler
    reads: under the adapter's key, which base.handle_message passes, and the runner's, which a multiplexed gateway
    recomputes (run_adapters.py:1745-1760). Returns the undo, for after the dispatch: it puts each old time back
    while the stamp is still ours, so a real message is acknowledged as before. Anything unexpected hushes nothing:
    the bubble then shows, as without this."""
    now = time.time()
    stamped = []
    try:
        runner = adapter.gateway_runner
        keys = {adapter._event_session_key(event)}
        try:
            keys.add(runner._session_key_for_source(event.source))
        except Exception:
            pass                                # the adapter's key still counts
        for key in keys:
            turn = getattr(runner._peek_session_state(key), "turn", None)
            if turn is not None:
                stamped.append((turn, turn.busy_ack_ts))
                turn.busy_ack_ts = now
    except Exception:
        pass                                    # whatever was stamped is put back by the undo

    def undo():
        for turn, before in stamped:
            if turn.busy_ack_ts == now:
                turn.busy_ack_ts = before
    return undo


def _holds_lines(text, length, digest, after=0):
    """Does text hold, as complete lines starting at or after index `after`, a run of exactly `length`
    characters with this fingerprint?"""
    if not isinstance(text, str):
        return False
    start = 0
    if after:
        start = text.find("\n", after - 1) + 1      # the first line that starts at or after it
        if start == 0:
            return False
    while start + length <= len(text):
        end = start + length
        if (end == len(text) or text[end] == "\n") and fingerprint(text[start:end]) == digest:
            return True
        start = text.find("\n", start) + 1
        if start == 0:
            break
    return False


def _waiting_marks(adapter, event):
    """The events waiting for this chat's next turns now (the slot and the runner's FIFO behind it),
    each with the length of its text: Hermes only ever appends to a waiting event, so one of them can
    hold a message recorded now only in text it gets after this point."""
    session = adapter._event_session_key(event)
    waiting = [(getattr(adapter, "_pending_messages", None) or {}).get(session)]
    overflow = getattr(getattr(adapter, "gateway_runner", None), "_overflow_queue", None)
    if callable(overflow):
        waiting.extend(overflow(session) or ())
    return tuple((weakref.ref(e), len(getattr(e, "text", None) or "")) for e in waiting if e is not None)


def _appended_from(seen, event):
    """Where the text appended to `event` since the message was recorded starts: 0 for an event that
    did not wait then (or a record kept by an older version, which has no marks)."""
    for ref, size in getattr(seen, "marks", None) or ():
        if ref() is event:
            return size
    return 0


def _merged_carrier(adapter, event, seen, message_id):
    """This chat's next-turn slot, when Hermes copied the message's text into it: a photo burst
    (run_busy.py) or the runner's busy merges (run_inbound.py) keep the event queued first and append
    the newer text, so the slot is no newer than the message and holds its whole text as complete
    lines, in what was appended after the message arrived. (Queue-mode debounce merges a separate
    notice into the same place by itself.)"""
    length = getattr(seen, "length", None)       # a record kept by an older version has none
    if not length:
        return None
    slot = (getattr(adapter, "_pending_messages", None) or {}).get(adapter._event_session_key(event))
    queued_as = numeric_id(getattr(slot, "message_id", None))
    if (queued_as is not None and int(queued_as) <= int(message_id)
            and _holds_lines(getattr(slot, "text", None), length, seen.fingerprint, _appended_from(seen, slot))):
        return slot
    return None


def _command_reading(event, text, message_type=None):
    """How Hermes reads this text, as (slash, runs).
    slash: it starts with '/', which Hermes' readers of queued text take for a command (the drain drops
    queued text that starts with a known one, run_turn.py), so it never replaces a waiting message.
    runs: sent as a new message, Hermes would take it as a command, never as conversation: by its own
    parse (MessageEvent.get_command: '/name ...', never a path such as /tmp/... or a bare '/'; an
    unknown name gets Hermes' 'Unknown command' reply), or as a plaintext admin phrase it turns into /restart
    (base.coerce_plaintext_gateway_command: kept from the agent, which restarting the gateway itself
    would leave stuck draining), which Hermes does only for a text message: message_type is the type Hermes
    gave the message, when the event at hand is not of it (a caption read for its media). The edit's own event
    is never changed."""
    probe = copy.copy(event)
    probe.text = text
    if message_type is not None:
        probe.message_type = message_type
    probe.allow_gateway_control = True
    runs = bool(probe.get_command())        # '' for a bare '/': Hermes then reads it as conversation
    if not runs:
        from gateway.platforms.base import coerce_plaintext_gateway_command
        coerce_plaintext_gateway_command(probe)
        runs = probe.text != text
    return text.lstrip().startswith("/"), runs


def _reads_as_command(event):
    """Does Hermes take what waits in this event as a command when it delivers it, by its own reading for the
    event's own message type?"""
    return _command_reading(event, event.text or "")[1]


def _media_of(msg):
    """(kind, file_unique_id) of a media message: what a caption notice calls it, and the stable id of the
    file Telegram sent (an animation also carries a document; it is named a GIF)."""
    photo = getattr(msg, "photo", None)
    if photo:
        return "photo", photo[-1].file_unique_id
    for attr, kind in (("animation", "GIF"), ("video", "video"), ("voice", "voice message"),
                       ("audio", "audio file"), ("document", "file")):
        item = getattr(msg, attr, None)
        if item is not None:
            return kind, getattr(item, "file_unique_id", None)
    return "file", None


def _waiting_events(adapter, event):
    """Every event still on its way to the agent that can carry one of this chat's media messages or an
    earlier caption notice: the photo-burst and album batches, the next-turn slots and the runner's FIFO
    behind this chat's slot. An event leaves them when its turn starts. A notice is popped from its text
    batch at once, and queue-mode debounce merges a later notice into an earlier one by itself."""
    events = []
    events.extend((getattr(adapter, "_pending_photo_batches", None) or {}).values())
    events.extend((getattr(adapter, "_media_group_events", None) or {}).values())
    events.extend((getattr(adapter, "_pending_messages", None) or {}).values())
    overflow = getattr(getattr(adapter, "gateway_runner", None), "_overflow_queue", None)
    if callable(overflow):
        events.extend(overflow(adapter._event_session_key(event)) or ())
    return [e for e in events if e is not None]


def _is_image_document(doc):
    """Hermes' test for a document it caches as an image and batches with photos (adapter._cache_inbound_document:
    an image extension, or an image mime type, which also covers a name without an extension)."""
    if doc is None:
        return False
    from plugins.platforms.telegram.adapter import _TELEGRAM_IMAGE_EXTENSIONS
    ext = os.path.splitext(getattr(doc, "file_name", None) or "")[1].lower()
    return ext in _TELEGRAM_IMAGE_EXTENSIONS or (getattr(doc, "mime_type", None) or "").lower().startswith("image/")


def _merged_batch(adapter, keyed, msg):
    """The batch Hermes merges a fresh media message into before the recorder runs, or None: an album item goes
    into its album (adapter._queue_media_group_event, keyed by media_group_id), a photo or an image document into
    its chat's photo burst (_route_photo_event, keyed by _photo_batch_key on the event as built, before topic
    recovery). Every other kind reaches Hermes' handle_message as its own event."""
    group = getattr(msg, "media_group_id", None)
    if group:
        return (getattr(adapter, "_media_group_events", None) or {}).get(str(group))
    if getattr(msg, "photo", None) or _is_image_document(getattr(msg, "document", None)):
        return (getattr(adapter, "_pending_photo_batches", None) or {}).get(adapter._photo_batch_key(keyed, msg))
    return None


def _media_carrier(adapter, event, msg, known, batch):
    """The waiting event that carries a media message Hermes just queued, and the path it cached the media
    at: the message's own event (Hermes queued it as built), else, in the batch Hermes merges this message into
    (_merged_batch), the one path no record knows (paths already recorded belong to earlier messages). (None,
    None) when it waits nowhere or that is ambiguous: a message whose own event waits nowhere (steered, held,
    straight to a turn) never takes another waiting event's path."""
    for candidate in _waiting_events(adapter, event):
        if getattr(candidate, "raw_message", None) is msg:
            urls = getattr(candidate, "media_urls", None) or []
            return candidate, (urls[0] if urls else None)
    if batch is None:
        return None, None
    unknown = {path for path in getattr(batch, "media_urls", None) or () if path not in known}
    if len(unknown) == 1:
        return batch, unknown.pop()
    return None, None


def _caption_carrier(adapter, event, seen):
    """Where a media message still waits: the event recorded as carrying it while that still waits, else
    whichever waiting event now holds its cached media (Hermes copies media_urls when it merges an event
    into another). None once its turn started, or when the plugin never saw it waiting."""
    if seen is None:
        return None
    waiting = _waiting_events(adapter, event)
    holder = seen.carrier()
    if holder is not None and any(candidate is holder for candidate in waiting):
        return holder
    path = (getattr(seen, "media", None) or (None, None, None))[2]
    # Only a message seen waiting is searched for by its path. A command caption's first edit has no holder;
    # after the separate notice that edit sent, its record's holder is that notice (handle_caption_edit), so a
    # later edit is searched by path too, which is harmless: the edit-time check still refuses a carrier that
    # reads as a command, and appending behind another caption is safe.
    if path and seen.holder is not None:
        return next((e for e in waiting if path in (getattr(e, "media_urls", None) or ())), None)
    return None


class Generation:
    """What one load of the plugin does; the wired handlers reach it through state.current."""

    def __init__(self, state, policy, gate, verified=True):
        self.state = state
        self.policy = policy
        self.gate = gate
        self.verified = verified          # Hermes matches the version this plugin was verified on
        self.cache = WordingCache(store=state.wording)

    def accepts_message(self, message):
        owner = self.policy.owner
        sender = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        return (
            self.verified
            and owner is not None
            and numeric_id(getattr(sender, "id", None)) == owner
            and numeric_id(getattr(chat, "id", None)) == owner
            and getattr(chat, "type", None) == "private"
            and getattr(message, "via_bot", None) is None
            and self.gate.enabled()
        )

    def record(self, update, adapter):
        """Runs after Hermes queued a fresh owner message (group 0 runs first; updates are FIFO per
        chat, and nothing awaits in between): remember the wording the agent will get, the text batch
        that now carries it and what already waited for the chat."""
        from gateway.platforms.event import MessageType
        from telegram.ext import filters
        msg = update.message
        carried = _carried_text(adapter, msg)
        built = adapter._build_message_event(msg, MessageType.TEXT)
        key = adapter._text_batch_key(built)          # Telegram's topic recovery, as Hermes batched it
        # Neither is in a batch that ends in its words, whatever the batch holds: Hermes holds a message
        # instead of batching it once teardown started, and batches a command only near the split limit
        # (a long paste), where a notice appended to it would become part of the command's arguments.
        if filters.COMMAND.check_update(update) or _tearing_down(adapter):
            carrier, sole = None, False
        else:
            carrier, sole = _batch_of(adapter, key, msg, carried)
        self.cache.put(msg.chat_id, msg.message_id, Seen(
            carried, fingerprint(carried), weakref.ref(carrier) if carrier is not None else None,
            fingerprint(carrier.text) if sole else None, len(carried), _waiting_marks(adapter, built)))

    async def handle_edit(self, update, context, adapter):
        """Deliver one owner edit. Returns False when Hermes' own handler should take the update,
        untagged. Raises only before anything was queued or changed, so the caller's fallback never
        delivers twice; after that, errors are only logged."""
        from gateway.platforms.event import MessageType
        msg = update.edited_message
        if not adapter._is_user_authorized_from_message(msg):
            return True                  # dropped, as Hermes' own handler would drop it
        if not adapter._should_process_message(msg):
            return True
        event = await adapter._build_triggered_event(msg, update, MessageType.TEXT)
        if not self.policy.accepts(event):
            return False
        key = adapter._text_batch_key(event)     # applies Telegram's topic recovery, as for any text
        corrected = event.text                   # Hermes' turn for the corrected message, notes included
        # Records hold the user's own words, as record() took them: Hermes' text for a reply to media also
        # names the media it cached for that turn (a new file each time), so it differs for the same words.
        words = _carried_text(adapter, msg)
        digest = fingerprint(words)
        seen = self.cache.get(msg.chat_id, msg.message_id)
        if seen is not None and seen.fingerprint == digest:
            return True                  # formatting only: the words the agent gets are unchanged
        slash, runs = _command_reading(event, corrected)
        carrier = seen.carrier() if seen is not None else None
        sole = False
        if carrier is not None and _waiting(adapter, event, carrier):
            # A correction that starts with '/' or reads as a command never replaces the waiting text:
            # Hermes' drain drops queued text that reads as a slash command (run_turn.py), and the agent
            # must read the notice saying a command was not run.
            sole = (fingerprint(carrier.text) == seen.holder_digest
                    and not getattr(carrier, "media_urls", None)
                    and not slash and not runs)
        elif seen is not None and seen.holder is not None:
            # Seen waiting, gone from there: Hermes may have copied its text into another waiting event.
            # (A message never seen waiting, like a command, is never searched for.)
            carrier = _merged_carrier(adapter, event, seen, numeric_id(msg.message_id))
        if (carrier is not None and not sole and fingerprint(carrier.text or "") != seen.holder_digest
                and _reads_as_command(carrier)):
            # What waits starts with another message's command (a long command Hermes batched, then this
            # message appended): a notice there would become that command's arguments. It goes on its own.
            # Exempt above: a carrier still holding only this message's own words, where appending voids them
            # as the owner's rule wants and a notice on its own would leave them to run. Those words read as a
            # command only as an admin phrase, or as a '/name' text Telegram gave no command entity (a
            # non-ASCII name such as '/über'): Hermes' get_command reads that as a command where PTB's COMMAND
            # filter doesn't, so record() gave it a carrier, and the notice joins its arguments.
            carrier = None
        changed = False
        try:
            if carrier is not None:
                # Still on its way to the agent: the correction goes with it, wherever it waits.
                if sole:
                    _set_waiting_text(carrier, corrected)     # the agent only ever gets the corrected text
                    carrier.allow_gateway_control = False
                else:
                    notice = self.policy.notice(event, seen.wording, command=runs)
                    _set_waiting_text(carrier, carrier.text + "\n" + notice)
                changed = True                   # from here on, handing the edit to Hermes would repeat it
                # Recorded at once: whatever fails later, the record describes the text now in place.
                self.cache.put(msg.chat_id, msg.message_id, Seen(
                    words, digest, weakref.ref(carrier), fingerprint(carrier.text) if sole else None,
                    len(words), _waiting_marks(adapter, event)))
                if sole:
                    self._rebase(carrier)
                return True
            event.text = self.policy.notice(event, seen.wording if seen is not None else None, command=runs)
            event.allow_gateway_control = False
            try:
                await _dispatch_now(adapter, key)    # what was sent first goes first (held in teardown)
            except asyncio.CancelledError:
                adapter._hold_inbound_event(event, where="edit-replacement-cancelled")   # after it
                raise
            marks = _waiting_marks(adapter, event)  # the notice can only be appended to these from now on
            adapter._enqueue_text_event(event)
            changed = True
            self.cache.put(msg.chat_id, msg.message_id, Seen(words, digest, weakref.ref(event), None,
                                                              len(words), marks))
            undo = _hush_busy_ack(adapter, event)   # a correction, not a new message: no busy acknowledgement
            try:
                await _dispatch_now(adapter, key)    # at once, nothing merges into it (held in teardown)
            finally:
                undo()
            return True
        except Exception as exc:
            if not changed:
                raise
            _log_failure("error after queueing an edit", exc)
            return True

    def _rebase(self, event):
        """The plugin just replaced a waiting event's text in place, the one write to a waiting event
        that is not an append: for every message recorded while it waited, only text appended from now
        on can hold that message."""
        size = len(event.text or "")
        for seen in self.cache.records():
            marks = getattr(seen, "marks", None) or ()
            if any(ref() is event for ref, _size in marks):
                seen.marks = tuple((ref, size) if ref() is event else (ref, n) for ref, n in marks)

    def _known_paths(self):
        """The cached media paths of every recorded media message: none of them can be a new message's."""
        return {(getattr(seen, "media", None) or (None, None, None))[2] for seen in self.cache.records()} - {None}

    def record_media(self, update, adapter):
        """Runs after Hermes queued a fresh owner media message (as record() for text): remember its caption
        words, which file it is, and where Hermes keeps it waiting. A caption Hermes reads as a command gets
        no carrier (a notice appended there would become the command's arguments, and Hermes' drain drops
        queued text that reads as a known command). A message held during teardown is in no waiting place,
        so it gets none either."""
        from gateway.platforms.event import MessageType
        msg = update.message
        words = _carried_text(adapter, msg) or ""
        built = adapter._build_message_event(msg, MessageType.TEXT)
        # Held during teardown, it was merged nowhere; else where Hermes merged it, keyed as Hermes keyed it.
        batch = None if _tearing_down(adapter) else _merged_batch(adapter, built, msg)
        adapter._text_batch_key(built)          # Telegram's topic recovery, as Hermes applied it to the media
        kind, file_id = _media_of(msg)
        _slash, runs = _command_reading(built, words, adapter._media_message_type(msg))   # as Hermes reads the media
        carrier, path = _media_carrier(adapter, built, msg, self._known_paths(), batch)
        if runs:
            carrier = None              # no notice joins it; its path stays known, so no later message takes it
        self.cache.put(msg.chat_id, msg.message_id, SeenMedia(
            words, fingerprint(words), weakref.ref(carrier) if carrier is not None else None, None, len(words),
            media=(kind, file_id, path)))

    async def handle_caption_edit(self, update, context, adapter):
        """Deliver one owner caption edit, never the media again. Hermes drops edited media, so declining
        delivers nothing. Raises only before anything was queued or changed; after that, errors are only
        logged."""
        from gateway.platforms.event import MessageType
        msg = update.edited_message
        if not adapter._is_user_authorized_from_message(msg):
            return
        if not adapter._should_process_message(msg):
            return
        # Built without downloading anything: no media handler, no replied-media cache.
        event = adapter._apply_telegram_group_observe_attribution(
            adapter._build_message_event(msg, MessageType.TEXT, update_id=update.update_id))
        words = _carried_text(adapter, msg) or ""
        event.text = words
        if not self.policy.accepts_caption(event):
            return
        key = adapter._text_batch_key(event)     # applies Telegram's topic recovery, as for any message
        digest = fingerprint(words)              # the caption words, as recorded
        kind, file_id = _media_of(msg)
        seen = self.cache.get(msg.chat_id, msg.message_id)
        media = getattr(seen, "media", None) or (None, None, None)
        if media[1] is not None and media[1] != file_id:
            return                       # the media itself was replaced: not a caption edit, left as Hermes leaves it
        if seen is not None and seen.fingerprint == digest:
            return                       # formatting only: the caption the agent got is unchanged
        _slash, runs = _command_reading(event, words, adapter._media_message_type(msg))   # as Hermes reads the media
        notice = self.policy.caption_notice(event, kind, seen.wording if seen is not None else None, words,
                                            command=runs)
        carrier = _caption_carrier(adapter, event, seen)
        if carrier is not None and _reads_as_command(carrier):
            # What waits starts with another item's command caption (a burst or album): a notice there would
            # become that command's arguments, and the drain drops a known command with all it carries.
            carrier = None
        changed = False
        try:
            if carrier is not None:
                # Still on its way to the agent: the correction goes with the media, in the same turn.
                _set_waiting_text(carrier, carrier.text + "\n" + notice if carrier.text else notice)
                changed = True
                self.cache.put(msg.chat_id, msg.message_id, SeenMedia(
                    words, digest, holder=weakref.ref(carrier), length=len(words),
                    media=(kind, file_id, media[2])))
                return
            event.allow_gateway_control = False
            event.text = notice
            try:
                await _dispatch_now(adapter, key)    # the chat's pending text batch first (held in teardown)
            except asyncio.CancelledError:
                adapter._hold_inbound_event(event, where="edit-replacement-caption-cancelled")   # after it
                raise
            adapter._enqueue_text_event(event)
            changed = True                           # queued: from here on errors are only logged
            self.cache.put(msg.chat_id, msg.message_id, SeenMedia(
                words, digest, holder=weakref.ref(event), length=len(words),
                media=(kind, file_id, media[2])))
            undo = _hush_busy_ack(adapter, event)    # a correction, not a new message: no busy acknowledgement
            try:
                await _dispatch_now(adapter, key)    # the notice itself, at once (held in teardown)
            finally:
                undo()
        except Exception as exc:
            if not changed:
                raise                                # nothing delivered yet: the handler logs it
            _log_failure("error after queueing a caption edit", exc)


def _batch_of(adapter, key, msg, carried):
    """The text batch that now carries a fresh message (Hermes' batch key: chat, topic, sender), and
    whether the message opened it (sole) rather than having its text appended as the last line."""
    pending = adapter._pending_text_batches.get(key)
    if pending is None:
        return None, False
    if getattr(pending, "raw_message", None) is msg:
        return pending, True
    if (pending.text or "").endswith("\n" + carried):
        return pending, False
    return None, False


def _quote(text):
    one_line = " ".join(text.split())
    if len(one_line) <= EARLIER_LIMIT:
        return one_line
    return one_line[:EARLIER_LIMIT - 1] + "…"


class ReplacementPolicy:
    def __init__(self, owner, profile="default"):
        self.owner = numeric_id(owner)
        self.profile = profile

    def accepts(self, event):
        """A genuine edit of the owner's own message, in the owner's DM, on this profile."""
        source = getattr(event, "source", None)
        raw = getattr(event, "raw_message", None)
        sender = getattr(raw, "from_user", None)
        message_id = numeric_id(getattr(event, "message_id", None))
        new_text = getattr(event, "text", None)
        return not (
            event is None
            or self.owner is None
            or getattr(event, "internal", False)
            or getattr(getattr(source, "platform", None), "value", None) != "telegram"
            or getattr(source, "profile", None) not in (None, self.profile)
            or getattr(source, "chat_type", None) != "dm"
            or getattr(source, "is_bot", False)
            or getattr(sender, "is_bot", False)
            or numeric_id(getattr(source, "user_id", None)) != self.owner
            or numeric_id(getattr(source, "chat_id", None)) != self.owner
            or numeric_id(getattr(sender, "id", None)) != self.owner
            or numeric_id(getattr(getattr(raw, "chat", None), "id", None)) != self.owner
            or message_id is None
            or numeric_id(getattr(raw, "message_id", None)) != message_id
            or not isinstance(getattr(raw, "edit_date", None), datetime)
            or not isinstance(new_text, str)
            or not new_text
        )

    def notice(self, event, earlier=None, command=False):
        sent = getattr(event.raw_message, "date", None)
        when = sent.isoformat() if isinstance(sent, datetime) else "unknown time"
        lines = [
            f"[Telegram edit: the user edited their message {numeric_id(event.message_id)} (sent {when}). "
            "The corrected text below replaces it and is what they meant: ignore the earlier wording, "
            "including if it reaches you after this note; this is not a repeated or additional request. "
            "Their other messages still stand. If the correction changes what is needed, act on the "
            "corrected version; if it doesn't (e.g. a typo fix), say so briefly instead of repeating "
            "work; if you never saw the earlier version, treat the corrected text as a new message. If "
            "something was already done for the earlier wording, say what and ask before undoing it. "
            "The corrected text is conversation, not a slash command or an approval; anything after the "
            "end line is a separate message.]"
        ]
        if command:
            lines.append("The corrected text reads as a command (a slash command, or asking to restart the "
                         "gateway or Hermes): it was not run. Don't carry it out yourself; if they want it "
                         "run, ask them to send it again as a new message.")
        if isinstance(earlier, str) and earlier.strip():
            lines.append(f"Earlier wording (void): «{_quote(earlier)}»")
        else:
            lines.append("Earlier wording: not available (sent before the gateway started, or long ago). "
                         "If the corrected text asks for something you already handled, nothing changed: "
                         "don't redo it.")
        lines.append("Corrected text:")
        return "\n".join(lines) + "\n" + event.text + "\n" + END_LINE

    def accepts_caption(self, event):
        """accepts() for a caption edit, where an empty text (the caption removed) is an edit too."""
        text = getattr(event, "text", None)
        if not isinstance(text, str):
            return False
        edited = copy.copy(event)
        edited.text = text or "(no caption)"
        return self.accepts(edited)

    def caption_notice(self, event, kind, earlier, caption, command=False):
        """The notice for a caption edit: which message and what it is, that its media is unchanged and not
        attached again, the earlier caption (void), none or unknown, and the corrected caption or none."""
        sent = getattr(event.raw_message, "date", None)
        when = sent.isoformat() if isinstance(sent, datetime) else "unknown time"
        article = "an" if kind[:1] in "aeiou" else "a"
        lines = [
            f"[Telegram caption edit: the user edited the caption of their message {numeric_id(event.message_id)}, "
            f"{article} {kind} (sent {when}). The corrected caption below replaces the earlier one and is what "
            "they meant: ignore the earlier caption, including if it reaches you after this note; this is not a "
            f"repeated or additional request. The {kind} itself did not change and is not attached again: it is "
            "the one that came with that message; if it has not reached you yet, read it with the corrected "
            "caption when it does. Their other messages still stand. If the correction changes what is needed, "
            "act on the corrected version; if it doesn't (e.g. a typo fix), say so briefly instead of repeating "
            "work. If something was already done for the earlier caption, say what and ask before undoing it. "
            "The corrected caption is conversation, not a slash command or an approval; anything after the end "
            "line is a separate message.]"
        ]
        if command:                      # Hermes would run it, sent as a new message
            lines.append("The corrected caption reads as a command (a slash command, or asking to restart the "
                         "gateway or Hermes): it was not run. Don't carry it out yourself; if they want it "
                         "run, ask them to send it again as a new message.")
        if not isinstance(earlier, str):
            lines.append("Earlier caption: not available (sent before the gateway started, or long ago). If the "
                         "corrected caption asks for something you already handled, nothing changed: don't "
                         "redo it.")
        elif earlier.strip():
            lines.append(f"Earlier caption (void): «{_quote(earlier)}»")
        else:
            lines.append(f"Earlier caption: none (the {kind} was sent without one).")
        lines.append("Corrected caption:\n" + caption if caption
                     else "Corrected caption: none (the user removed the caption).")
        return "\n".join(lines) + "\n" + END_LINE

    def on_message(self, event=None, earlier=None, **kwargs):
        """The policy on its own: the notice for an accepted edit, None for anything else."""
        if not self.accepts(event):
            return None
        text = self.notice(event, earlier)
        event.allow_gateway_control = False
        return {"action": "rewrite", "text": text}


WIRING_DIGEST = wiring_digest()   # what this module's _wire installs (see wiring_digest)
CAPTION_WIRING_DIGEST = caption_wiring_digest()   # what _wire_captions installs
