"""Runs the plugin through real PTB dispatch, text batching and the busy lane, and through Hermes' own
plugin loading, forced reload (the control socket's reload-plugins verb), CLI disable/enable and
late re-wire (the gateway runner's GatewayPluginRewireMixin) paths, offline. Network, model calls
and real profile storage are never used: called in a child whose HERMES_HOME is a
TemporaryDirectory, before any Hermes import.
"""
import asyncio
import contextlib
import copy
import dataclasses
import importlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import types
import unittest

# Importing gateway.run (the reload verb does) imports hermes_bootstrap, whose launch preparation can
# finish a pending self-update (a dependency sync on the live install) or os.execv this process under
# the store interpreter. A test child must do neither.
os.environ["HERMES_DISABLE_LAZY_INSTALLS"] = "1"
# The Hermes these tests run is the one scripts/pin_hermes.py pins: one definition (this script's dir is
# sys.path[0]).
from pin_hermes import REPO
sys.path.insert(0, str(REPO))

class Mark:
    def __getattr__(self, name):
        def decorate(*a, **k):
            return a[0] if len(a) == 1 and callable(a[0]) and not k else (lambda f: f)
        return decorate
stub = types.ModuleType("pytest")
stub.__dict__.update(mark=Mark(),
    importorskip=lambda name, *a, **k: importlib.import_module(name),
    fixture=lambda *a, **k: a[0] if a and callable(a[0]) else (lambda f: f),
    param=lambda *a, **k: a)
sys.modules.setdefault("pytest", stub)

spec = importlib.util.spec_from_file_location("offline_tg_helpers", REPO / "tests/plugins/test_telegram_update_admission.py")
assert spec is not None and spec.loader is not None
T = importlib.util.module_from_spec(spec)
spec.loader.exec_module(T)

from hermes_cli.plugins import discover_plugins, get_plugin_manager
from gateway.run import GatewayRunner
from gateway.run_busy import GatewayBusySessionMixin
from gateway.session_state import SessionState
from gateway.run_plugin_rewire import GatewayPluginRewireMixin, reload_plugins_verb
# Load via Hermes itself, not a fake context or a directly imported policy.
discover_plugins()
manager = get_plugin_manager()
assert manager.get_platform_handler_factories("telegram"), "plugin factory was not registered"

KEY = "telegram-edit-replacement"
LOGGER = "hermes.plugins.telegram_edit_replacement"
HOME = Path(os.environ["HERMES_HOME"])
CONFIG = HOME / "config.yaml"
ENABLED_CONFIG = CONFIG.read_text()
INSTALLED = HOME / "plugins" / KEY / "__init__.py"
STATE_MODULE = "_hermes_telegram_edit_replacement_state"
MARK = "[Telegram edit:"
NOT_RUN = "it was not run"
IGNORE = "ignore the earlier wording"
END = "[End of the corrected message.]"
_stamp = [time.time_ns()]


class MP:
    def __init__(self):
        self.undo = []
    def setattr(self, obj, key, value):
        self.undo.append((obj, key, getattr(obj, key)))
        setattr(obj, key, value)
    def setenv(self, key, value):
        self.undo.append((os.environ, key, os.environ.get(key)))
        os.environ[key] = value
    def restore(self):
        for obj, key, value in reversed(self.undo):
            if obj is os.environ:
                if value is None:
                    obj.pop(key, None)
                else:
                    obj[key] = value
            else:
                setattr(obj, key, value)
        self.undo.clear()


class Runner(GatewayPluginRewireMixin):
    """The gateway runner's plugin re-wire half, holding the adapters it would re-wire."""
    def __init__(self, adapters=None):
        self.adapters = adapters or {}


class BusyRunner(GatewayBusySessionMixin):
    """The runner's real next-turn queue (the adapter's pending slot, then a FIFO in session state),
    for a busy turn whose agent is still being set up: steer and interrupt modes then queue a plain
    text for the next turn instead of steering it (run_busy.py:614-623, 861-863). merge=True takes
    the runner's other busy path instead, which copies a text follow-up into the slot's event
    (run_inbound.py:641, 714: the post-start grace window, an agent still being set up)."""
    _BUSY_QUEUE_MAX_PENDING = GatewayRunner._BUSY_QUEUE_MAX_PENDING
    _draining = False

    def __init__(self, adapter, merge=False):
        self.adapter = adapter
        self.merge = merge
        self.states = {}
        self.calls = 0
        self.steering = False             # True: the turn's agent is live and plain text is steered into it
        self.steer_declined = False       # True: that agent doesn't take the steer (its steer() fails)
        self.cancel_after_steer = False
        self.steered = []
        self.interrupting = False         # True: interrupt mode with a live agent (run_busy.py:876-880)
        self.interrupts = []
        self.acks = []                    # the events Hermes answered with its busy acknowledgement

    def _pending_event_audio_paths(self, event):
        return []                         # with_stt() binds Hermes' own

    def _delivery_adapter_for(self, source):
        return self.adapter

    def _session_state(self, key):
        return self.states.setdefault(key, SessionState())      # Hermes' own per-session state

    def _peek_session_state(self, key):
        return self.states.get(key)

    def _profile_name_for_source(self, source, adapter_profile=None):
        return None                 # no profile routes: the adapter's own profile, as without a runner

    def _ack(self, event, session_key):
        """Hermes' busy acknowledgement, which ends every routing below (run_busy.py:882-905): "⏩ Steered into the
        current run ..." (or queued, interrupting), at most once per 30 s of a turn, stamped on the turn's
        busy_ack_ts. test_hermes_acknowledges_a_busy_message_at_most_once_per_30_seconds pins it to Hermes' own."""
        turn = self._session_state(session_key).turn
        now = time.time()
        if now - turn.busy_ack_ts < 30:
            return
        turn.busy_ack_ts = now
        self.acks.append(event)

    async def busy(self, event, session_key):
        self.calls += 1
        plain = event.message_type == T.MessageType.TEXT and not event.media_urls and not event.media_types
        # run_busy.py:605-618: plain text, or media that is all voice (STT input), is steered into the turn
        all_voice = bool(event.media_urls) and len(self._pending_event_audio_paths(event)) == len(event.media_urls)
        if self.steering and (plain or all_voice):
            # run_busy.py:605-623: Hermes prepares the steer text (transcribing voice), steers only a non-empty
            # text into an agent that takes it, and otherwise falls back to queue mode below, the transcript it
            # just made staying cached on the event.
            steer_text = (await self._prepare_busy_steer_text(event)) if all_voice else event.text
            if steer_text and not self.steer_declined:
                self.steered.append(event)          # into the running turn, never queued
                if self.cancel_after_steer:
                    raise asyncio.CancelledError    # a cancel that lands right after the steer
                self._ack(event, session_key)
                return True
        if self.merge:
            GatewayRunner._hm_merge_pending_for_source(self, event.source, session_key, event, merge_text=True)
        else:
            self._queue_or_replace_pending_event(session_key, event)
        if self.interrupting:                   # Hermes' interrupt of the live agent, transcribing voice first
            agent = types.SimpleNamespace(interrupt=self.interrupts.append)
            await self._interrupt_running_agent_for_busy_event(event, self.adapter, agent)
        if not self.merge:                      # run_inbound's grace-window merge sends no busy acknowledgement
            self._ack(event, session_key)
        return True

    async def next_turn(self, key, source):
        """What the runner does when the running turn ends: take the next queued input, with its rule
        that queued text reading as a slash command is dropped (run_turn.py _run_agent_drain_pending)."""
        return await GatewayRunner._run_agent_drain_pending(self, {"final_response": "done"}, self.adapter,
                                                            source, key)


def with_stt(runner):
    """Give a BusyRunner Hermes' real speech-to-text path for queued voice notes: which media are STT input, the
    transcribe-once cache on the event, the echo ledger, the enrichment text. Only the provider call
    (_transcribe_one_clip) and Telegram's send of the echo are stood in for. Returns (clips, echoes): every clip
    transcribed, and every transcript echoed to the chat."""
    clips, echoes = [], []

    async def transcribe_one_clip(path, transcribe_audio, fallback):
        clips.append(path)
        return f"spoken {len(clips)}", f'"spoken {len(clips)}"'

    async def echo(adapter, source, transcripts, *, metadata=None, log_context="Transcript"):
        echoes.extend(transcripts)          # one message per transcript, none for an empty list

    for name in ("_pending_event_audio_paths", "_transcribe_pending_audio_event_once",
                 "_transcribe_and_echo_pending_voice", "_echo_pending_stt_transcripts_once",
                 "_enrich_message_with_transcription", "_should_echo_stt_transcripts"):
        setattr(runner, name, types.MethodType(getattr(GatewayRunner, name), runner))
    runner._prepend_media_prefix = GatewayRunner._prepend_media_prefix
    runner.config = types.SimpleNamespace(stt_enabled=True, stt_echo_transcripts=True)
    runner._transcribe_one_clip = transcribe_one_clip
    runner._echo_stt_transcripts = echo
    runner._thread_metadata_for_source = lambda source, reply_to=None: None
    runner._reply_anchor_for_event = lambda event: None
    return clips, echoes


async def drain(adapter):
    for _ in range(3):
        tasks = list(adapter._pending_text_batch_tasks.values())
        if tasks:
            await asyncio.gather(*tasks)
        await asyncio.sleep(0)

def upd(bot, uid, text, *, edited=False, mid=472, user=88, chat=88, command=False, entities=None, via_bot=False):
    update = T.update(bot, uid, kind="command" if command else "text", text=text,
                      edited=edited, user=user, chat=chat)
    # Build through Telegram JSON again to keep actual immutable PTB Message objects.
    data = update.to_dict()
    msg = data["edited_message" if edited else "message"]
    msg["message_id"] = mid
    msg["text"] = text
    if entities is not None:
        msg["entities"] = entities
    if via_bot:
        msg["via_bot"] = {"id": 5, "is_bot": True, "first_name": "Inline", "username": "inline_bot"}
    return T.Update.de_json(data, bot)


def reply_upd(bot, uid, text, **kwargs):
    """upd() for a message that replies to a photo the bot sent: Hermes caches that photo and appends a
    note naming it to the turn's text (adapter._cache_replied_media)."""
    data = upd(bot, uid, text, **kwargs).to_dict()
    msg = data["edited_message" if kwargs.get("edited") else "message"]
    msg["reply_to_message"] = {
        "message_id": 650, "date": 1800000000, "chat": dict(msg["chat"]),
        "from": {"id": 111, "is_bot": True, "first_name": "Offline", "username": "offline_bot"},
        "photo": [{"file_id": "replied", "file_unique_id": "replied", "width": 1, "height": 1, "file_size": 10}],
    }
    return T.Update.de_json(data, bot)


def corrected(text):
    return "\nCorrected text:\n" + text + "\n" + END


def corrected_start(text):
    """The start of a notice's corrected text, for a turn Hermes appends a note to after the words."""
    return "\nCorrected text:\n" + text + "\n"


MEDIA_PAYLOADS = {
    "photo": lambda f: {"photo": [{"file_id": f, "file_unique_id": f, "width": 1, "height": 1, "file_size": 10}]},
    "document": lambda f: {"document": {"file_id": f, "file_unique_id": f, "file_name": "report.pdf",
                                        "mime_type": "application/pdf", "file_size": 20}},
    "image document": lambda f: {"document": {"file_id": f, "file_unique_id": f, "file_name": "shot.png",
                                              "mime_type": "image/png", "file_size": 20}},
    "nameless image document": lambda f: {"document": {"file_id": f, "file_unique_id": f, "file_name": "screenshot",
                                                       "mime_type": "image/png", "file_size": 20}},
    "text document": lambda f: {"document": {"file_id": f, "file_unique_id": f, "file_name": "notes.txt",
                                             "mime_type": "text/plain", "file_size": 20}},
    "video": lambda f: {"video": {"file_id": f, "file_unique_id": f, "width": 1, "height": 1, "duration": 1,
                                  "mime_type": "video/mp4", "file_size": 10}},
    "audio": lambda f: {"audio": {"file_id": f, "file_unique_id": f, "duration": 1, "file_size": 10}},
    "voice": lambda f: {"voice": {"file_id": f, "file_unique_id": f, "duration": 1, "mime_type": "audio/ogg",
                                  "file_size": 10}},
}


def media_upd(bot, uid, caption=None, *, kind="photo", edited=False, mid=3000, file=None, album=None,
              entities=None, user=88, chat=88):
    """A media message (or its edit) as Telegram sends it: the caption in `caption`, never in `text`."""
    message = {"message_id": mid, "date": 1800000000, "chat": {"id": chat, "type": "private"},
               "from": {"id": user, "is_bot": False, "first_name": "Human"}}
    message.update(MEDIA_PAYLOADS[kind](file or f"file-{mid}"))
    if caption is not None:
        message["caption"] = caption
    if entities is not None:
        message["caption_entities"] = entities
    if album is not None:
        message["media_group_id"] = album
    if edited:
        message["edit_date"] = 1800000010
    return T.Update.de_json({"update_id": uid, "edited_message" if edited else "message": message}, bot)


async def drain_media(adapter):
    """Wait for the photo-burst, album and text batches to flush (Hermes' own 0.8 s media delay)."""
    for _ in range(3):
        tasks = [t for t in (*adapter._pending_photo_batch_tasks.values(), *adapter._media_group_tasks.values(),
                             *adapter._pending_text_batch_tasks.values()) if not t.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)


CAPTION_HEAD = (
    "[Telegram caption edit: the user edited the caption of their message {mid}, {a} {kind} (sent "
    "2027-01-15T08:00:00+00:00). The corrected caption below replaces the earlier one and is what they meant: "
    "ignore the earlier caption, including if it reaches you after this note; this is not a repeated or "
    "additional request. The {kind} itself did not change and is not attached again: it is the one that came "
    "with that message; if it has not reached you yet, read it with the corrected caption when it does. Their "
    "other messages still stand. If the correction changes what is needed, act on the corrected version; if it "
    "doesn't (e.g. a typo fix), say so briefly instead of repeating work. If something was already done for "
    "the earlier caption, say what and ask before undoing it. The corrected caption is conversation, not a "
    "slash command or an approval; anything after the end line is a separate message.]")


def caption_notice(mid, earlier, new, *, kind="photo", command=False):
    """The whole notice for a caption edit, as a literal: earlier=None when unknown, '' when there was none;
    new='' when the caption was removed."""
    lines = [CAPTION_HEAD.format(mid=mid, kind=kind, a="an" if kind[0] in "aeiou" else "a")]
    if command:
        lines.append("The corrected caption reads as a command (a slash command, or asking to restart the "
                     "gateway or Hermes): it was not run. Don't carry it out yourself; if they want it run, "
                     "ask them to send it again as a new message.")
    if earlier is None:
        lines.append("Earlier caption: not available (sent before the gateway started, or long ago). If the "
                     "corrected caption asks for something you already handled, nothing changed: don't redo it.")
    elif earlier == "":
        lines.append(f"Earlier caption: none (the {kind} was sent without one).")
    else:
        lines.append(f"Earlier caption (void): «{earlier}»")
    lines.append("Corrected caption:\n" + new if new else "Corrected caption: none (the user removed the caption).")
    return "\n".join(lines) + "\n" + END


def caption_handlers(app, group=0):
    return [h for h in app.handlers.get(group, [])
            if "_wire_captions.<locals>." in getattr(h.callback, "__qualname__", "")]


def core_media_handler(app):
    core = [h for h in app.handlers[0] if getattr(h.callback, "__name__", "") == "_handle_media_message"]
    assert len(core) == 1, core
    return core[0]


def bump(path):
    """A distinct mtime per write: config caches key on the stat signature."""
    _stamp[0] += 10**9
    os.utime(path, ns=(_stamp[0], _stamp[0]))


def write_selection(enabled, disabled):
    """What `hermes plugins enable/disable` commit: the selection lists in config.yaml, nothing else.
    Tests never call those commands: their package-manager admission transaction runs against the live
    Hermes install (shared by every HERMES_HOME) and spawns a worker by re-running sys.argv[0]."""
    CONFIG.write_text("plugins:\n"
                      f"  enabled: [{', '.join(enabled)}]\n"
                      f"  disabled: [{', '.join(disabled)}]\n"
                      "  entries:\n    telegram-edit-replacement:\n      settings:\n        owner_user_id: '88'\n")
    bump(CONFIG)


def load_installed_plugin(name):
    path = INSTALLED
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    pkg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pkg)
    return pkg


def handler_count(app):
    return sum(len(hs) for hs in app.handlers.values())


def edit_handlers(app):
    return [h for h in app.handlers.get(0, [])
            if "_wire.<locals>.edited" in getattr(h.callback, "__qualname__", "")]


def core_text_handler(app):
    core = [h for h in app.handlers[0] if getattr(h.callback, "__name__", "") == "_handle_text_message"]
    assert len(core) == 1, core
    return core[0]


def main_state(app):
    holder = sys.modules.get(STATE_MODULE)
    assert holder is not None, "the plugin keeps no process-wide state"
    found = [state for state in holder.profiles.values() if app in state.wired]
    assert len(found) == 1, "exactly one plugin state wired this Telegram client"
    return found[0]


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mp = MP()
        self.unsubscribe = []
        holder = sys.modules.get(STATE_MODULE)
        for state in (holder.profiles.values() if holder is not None else ()):
            state.wording.clear()       # every test starts with no remembered messages
            getattr(state, "snapshots", {}).clear()
        self.cm = T.connected(self.mp)
        self.adapter, self.app, self.delivered = await self.cm.__aenter__()

    async def asyncTearDown(self):
        for unsubscribe in self.unsubscribe:
            unsubscribe()
        await self.cm.__aexit__(None, None, None)
        self.mp.restore()

    def subscribe(self, adapter):
        """The runner's on_plugin_loaded subscription, as the gateway installs it per served profile."""
        runner = Runner({"telegram": adapter})
        runner._subscribe_plugin_rewire(manager)
        self.unsubscribe.extend((runner._plugin_rewire_unsubscribe or {}).values())
        return runner

    async def reload(self):
        """What the gateway runs for `reload-plugins` (CLI enable/install, Desktop, plugins.manage): a
        forced discovery on the control socket's executor thread."""
        verb = reload_plugins_verb(Runner(), asyncio.get_running_loop())
        answer = await asyncio.to_thread(verb, {})
        self.assertTrue(answer.get("reloaded"), answer)
        await asyncio.sleep(0)          # a re-wire that on_plugin_loaded scheduled on this loop
        return answer

    async def restore_enabled(self):
        CONFIG.write_text(ENABLED_CONFIG)
        bump(CONFIG)
        await self.reload()

    @contextlib.contextmanager
    def busy(self, runner=None, mode=None, key=None):
        """The owner's session busy, as while a turn runs; yields its session key."""
        key = key or self.adapter._event_session_key(self.adapter._build_message_event(
            upd(self.app.bot, 299, "dummy").message, T.MessageType.TEXT))
        missing = object()
        saved = {name: self.adapter.__dict__.get(name, missing) for name in ("gateway_runner", "_busy_text_mode")}
        self.adapter._active_sessions[key] = asyncio.Event()
        self.adapter._session_tasks[key] = asyncio.current_task()
        if mode is not None:
            self.adapter._busy_text_mode = mode
        if runner is not None:
            self.adapter.gateway_runner = runner
            if hasattr(runner, "_session_state"):
                runner._session_state(key)        # a running turn has its session state (Hermes makes it at the start)
        self.adapter.set_busy_session_handler(runner.busy if runner is not None else None)
        try:
            yield key
        finally:
            state = self.adapter._text_debounce_store().pop(key, None)
            if state is not None:
                state.cancel_timer()
            self.adapter._pending_messages.pop(key, None)
            self.adapter._session_tasks.pop(key, None)
            self.adapter._active_sessions.pop(key, None)
            for name, value in saved.items():
                if value is missing:
                    self.adapter.__dict__.pop(name, None)
                else:
                    setattr(self.adapter, name, value)

    def queue_photo(self, runner, key, mid):
        """A photo already queued for the session's next turn."""
        photo = self.adapter._build_message_event(upd(self.app.bot, 0, "placeholder", mid=mid).message,
                                                  T.MessageType.PHOTO)
        photo.text = ""
        photo.media_urls, photo.media_types = ["offline.png"], ["image/png"]
        runner._queue_or_replace_pending_event(key, photo)
        return photo

    async def teardown_and_reconnect(self):
        """What disconnect does after it set the drop flag, then a reconnect's redispatch."""
        await self.adapter._cancel_pending_delivery_tasks()
        self.adapter._drop_delayed_deliveries = False
        await self.adapter._redispatch_held_inbound()
        await drain(self.adapter)

    async def test_edit_is_tagged_before_idle_dispatch_and_replay_is_once(self):
        await self.app.process_update(upd(self.app.bot, 100, "old wording"))
        await drain(self.adapter)
        with self.assertNoLogs(LOGGER, "WARNING"):
            await self.app.process_update(upd(self.app.bot, 101, "new wording", edited=True))
            await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].text, "old wording")
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertIn(IGNORE, self.delivered[1].text)
        self.assertIn("Earlier wording (void): «old wording»", self.delivered[1].text)
        self.assertTrue(self.delivered[1].text.endswith(corrected("new wording")))
        self.assertFalse(self.delivered[1].allow_gateway_control)
        await self.app.process_update(upd(self.app.bot, 101, "new wording", edited=True))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)

    async def test_rapid_edit_replaces_the_unsent_original_outright(self):
        await self.app.process_update(upd(self.app.bot, 200, "old value"))
        await self.app.process_update(upd(self.app.bot, 201, "latest value", edited=True))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["latest value"],
                         "the agent never saw the original: it gets the corrected text as the message")
        self.assertFalse(self.delivered[0].allow_gateway_control)
        # What the agent saw is now "latest value": a later edit replaces THAT wording.
        await self.app.process_update(upd(self.app.bot, 202, "final value", edited=True))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertIn("Earlier wording (void): «latest value»" + corrected("final value"), self.delivered[1].text)

    async def test_a_second_rapid_edit_replaces_the_unsent_original_again(self):
        await self.app.process_update(upd(self.app.bot, 205, "first try"))
        await self.app.process_update(upd(self.app.bot, 206, "second try", edited=True))
        await self.app.process_update(upd(self.app.bot, 207, "third try", edited=True))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["third try"],
                         "still unsent after two edits: the agent gets only the latest text, no notice")
        self.assertFalse(self.delivered[0].allow_gateway_control)

    async def test_rapid_edit_into_a_command_is_not_run(self):
        await self.app.process_update(upd(self.app.bot, 210, "hello there", mid=474))
        await self.app.process_update(upd(self.app.bot, 211, "/status", edited=True, command=True, mid=474))
        await drain(self.adapter)
        [event] = self.delivered
        # Never '/status' alone: Hermes' drain drops queued text that reads as a command (logic below).
        self.assertTrue(event.text.startswith("hello there\n" + MARK), event.text)
        self.assertTrue(event.text.endswith(corrected("/status")))
        self.assertIsNone(event.get_command(), "an edit never runs a slash command")

    async def test_different_pending_message_is_delivered_first(self):
        await self.app.process_update(upd(self.app.bot, 220, "keep other message", mid=471))
        await self.app.process_update(upd(self.app.bot, 221, "correct this one", edited=True, mid=472))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].text, "keep other message")
        self.assertTrue(self.delivered[0].allow_gateway_control)
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertTrue(self.delivered[1].text.endswith(corrected("correct this one")))
        self.assertFalse(self.delivered[1].allow_gateway_control)

    def assert_in_order(self, text, *parts):
        positions = [text.index(part) for part in parts]
        self.assertEqual(positions, sorted(positions), text)

    async def test_edit_of_a_waiting_mixed_batch_travels_with_it(self):
        await self.app.process_update(upd(self.app.bot, 230, "earlier wording", mid=472))
        await self.app.process_update(upd(self.app.bot, 231, "unrelated followup", mid=473))
        await self.app.process_update(upd(self.app.bot, 232, "corrected wording", edited=True, mid=472))
        await drain(self.adapter)
        [batch] = self.delivered
        self.assert_in_order(batch.text, "earlier wording", "unrelated followup", MARK)
        self.assertIn("Earlier wording (void): «earlier wording»" + corrected("corrected wording"), batch.text)
        self.assertTrue(batch.allow_gateway_control, "the user's own messages keep gateway control")

    async def test_edit_of_a_waiting_message_with_media_travels_with_it(self):
        await self.app.process_update(upd(self.app.bot, 240, "old text", mid=472))
        pending = next(iter(self.adapter._pending_text_batches.values()))
        pending.media_urls = ["offline-reference.png"]
        pending.media_types = ["image/png"]
        await self.app.process_update(upd(self.app.bot, 241, "new text", edited=True, mid=472))
        await drain(self.adapter)
        [event] = self.delivered
        self.assertEqual(event.media_urls, ["offline-reference.png"], "media is never dropped")
        self.assert_in_order(event.text, "old text", MARK)
        self.assertTrue(event.text.endswith(corrected("new text")))

    async def test_pending_batch_the_plugin_never_recorded_is_delivered_first(self):
        await self.app.process_update(upd(self.app.bot, 250, "queued before load", mid=470))
        state = main_state(self.app)
        state.wording.clear()                       # as if the plugin loaded after it was queued
        getattr(state, "snapshots", {}).clear()
        await self.app.process_update(upd(self.app.bot, 251, "fixed", edited=True, mid=470))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].text, "queued before load")
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertIn("Earlier wording: not available", self.delivered[1].text)
        self.assertTrue(self.delivered[1].text.endswith(corrected("fixed")))

    async def test_unobserved_pending_batch_is_never_dropped(self):
        await self.app.process_update(upd(self.app.bot, 260, "same words", mid=460))
        await drain(self.adapter)
        # A batch queued on a path the recorder never sees (e.g. a held event re-dispatched), holding
        # a different message with the same words.
        other = upd(self.app.bot, 261, "same words", mid=461).message
        self.adapter._enqueue_text_event(self.adapter._build_message_event(other, T.MessageType.TEXT))
        await self.app.process_update(upd(self.app.bot, 262, "different words", edited=True, mid=460))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered][:2], ["same words", "same words"])
        self.assertEqual(len(self.delivered), 3)
        self.assertTrue(self.delivered[2].text.startswith(MARK))

    async def test_original_with_a_hidden_link_is_still_replaced(self):
        link = [{"type": "text_link", "offset": 4, "length": 4, "url": "https://example.com/docs"}]
        await self.app.process_update(upd(self.app.bot, 270, "see docs now", mid=450, entities=link))
        pending = next(iter(self.adapter._pending_text_batches.values()))
        self.assertNotEqual(pending.text, "see docs now", "Hermes expands the hidden link")
        await self.app.process_update(upd(self.app.bot, 271, "see the docs now", edited=True, mid=450))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["see the docs now"])

    async def test_unchanged_text_is_not_delivered_again(self):
        await self.app.process_update(upd(self.app.bot, 280, "keep me", mid=440))
        await drain(self.adapter)
        bold = [{"type": "bold", "offset": 0, "length": 4}]
        await self.app.process_update(upd(self.app.bot, 281, "keep me", edited=True, mid=440, entities=bold))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1, "a formatting-only edit changes nothing the agent saw")
        await self.app.process_update(upd(self.app.bot, 282, "keep me please", edited=True, mid=440))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertTrue(self.delivered[1].text.endswith(corrected("keep me please")))

    async def test_teardown_keeps_what_was_sent_first_first(self):
        await self.app.process_update(upd(self.app.bot, 290, "other pending message", mid=431))
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)   # what disconnect sets first
        await self.app.process_update(upd(self.app.bot, 291, "edited older message", edited=True, mid=430))
        self.assertEqual(self.delivered, [], "teardown: Hermes holds, never dispatches, now")
        await self.teardown_and_reconnect()
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].text, "other pending message")
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertTrue(self.delivered[1].text.endswith(corrected("edited older message")))

    async def test_teardown_edit_of_a_waiting_message_travels_with_it(self):
        await self.app.process_update(upd(self.app.bot, 292, "meet at 5pm", mid=480))
        await self.app.process_update(upd(self.app.bot, 293, "and bring the files", mid=481))
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)
        await self.app.process_update(upd(self.app.bot, 294, "meet at 6pm", edited=True, mid=480))
        await self.teardown_and_reconnect()
        [batch] = self.delivered
        self.assert_in_order(batch.text, "meet at 5pm", "and bring the files", MARK)
        self.assertIn("Earlier wording (void): «meet at 5pm»" + corrected("meet at 6pm"), batch.text)

    async def test_edit_of_a_later_message_in_a_waiting_batch_travels_with_it(self):
        await self.app.process_update(upd(self.app.bot, 2100, "first part", mid=590))
        await self.app.process_update(upd(self.app.bot, 2101, "second part", mid=591))
        await self.app.process_update(upd(self.app.bot, 2102, "second part, fixed", edited=True, mid=591))
        await drain(self.adapter)
        [batch] = self.delivered
        self.assert_in_order(batch.text, "first part", "second part", MARK)
        self.assertIn("Earlier wording (void): «second part»" + corrected("second part, fixed"), batch.text)

    async def test_message_right_after_an_edit_stays_separate(self):
        await self.app.process_update(upd(self.app.bot, 2000, "first version", mid=580))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 2001, "second version", edited=True, mid=580))
        await self.app.process_update(upd(self.app.bot, 2002, "yes", mid=581))   # inside the batch window
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 3)
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertEqual(self.delivered[2].text, "yes")
        self.assertTrue(self.delivered[2].allow_gateway_control, "a new message keeps gateway control")

    async def test_error_after_queueing_never_delivers_twice(self):
        cache = main_state(self.app).current.cache

        def broken(*args, **kwargs):
            raise RuntimeError("simulated defect after queueing")

        cache.put = broken
        try:
            with self.assertLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 2200, "only once", edited=True, mid=600))
        finally:
            del cache.put
        await drain(self.adapter)          # the queued edit still goes out on Hermes' own timer
        self.assertEqual(len(self.delivered), 1)
        self.assertTrue(self.delivered[0].text.endswith(corrected("only once")))
        self.assertEqual(self.delivered[0].text.count("only once"), 1)

    async def test_errors_in_loaded_code_leave_updates_to_hermes(self):
        generation = main_state(self.app).current

        def boom(*args, **kwargs):
            raise RuntimeError("simulated defect in newly loaded code")

        async def boom_async(*args, **kwargs):
            boom()

        phases = [("accepts_message", boom, upd(self.app.bot, 1800, "filter broke", edited=True, mid=560)),
                  ("handle_edit", boom_async, upd(self.app.bot, 1801, "handler broke", edited=True, mid=561)),
                  ("record", boom, upd(self.app.bot, 1802, "recorder broke", mid=562))]
        for attribute, broken, update in phases:
            with self.subTest(attribute):
                setattr(generation, attribute, broken)
                try:
                    with self.assertLogs(LOGGER, "WARNING"):
                        await self.app.process_update(update)
                        await drain(self.adapter)
                finally:
                    delattr(generation, attribute)
                self.assertEqual(self.delivered[-1].text, update.effective_message.text)
        self.assertEqual(len(self.delivered), 3)

    async def test_edit_declined_by_the_policy_goes_to_hermes(self):
        real = self.adapter._build_triggered_event
        calls = []

        async def first_is_internal(msg, update, msg_type):
            event = await real(msg, update, msg_type)
            calls.append(update.update_id)
            if len(calls) == 1:
                event.internal = True       # something the policy re-checks on the built event
            return event

        self.mp.setattr(self.adapter, "_build_triggered_event", first_is_internal)
        await self.app.process_update(upd(self.app.bot, 1900, "declined edit", edited=True, mid=570))
        await drain(self.adapter)
        self.assertEqual(calls, [1900, 1900])
        self.assertEqual([e.text for e in self.delivered], ["declined edit"])
        self.assertTrue(self.delivered[0].allow_gateway_control)

    async def test_edit_of_a_message_waiting_for_the_next_turn_replaces_it_there(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3000, "deploy to staging", mid=620))
            await drain(self.adapter)
            queued = self.adapter._pending_messages[key]
            self.assertEqual(queued.text, "deploy to staging")
            await self.app.process_update(upd(self.app.bot, 3001, "deploy to production", edited=True, mid=620))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], queued)
            self.assertEqual(queued.text, "deploy to production", "the agent never saw the original")
            self.assertFalse(queued.allow_gateway_control)
            self.assertEqual(runner.calls, 1, "the edit never overtakes the message it corrects")
            self.assertEqual(self.delivered, [])

    async def test_edit_of_a_message_in_the_runners_fifo_replaces_it_there(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3010, "first request", mid=630))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3011, "second request", mid=631))
            await drain(self.adapter)
            overflow = runner._overflow_queue(key)
            self.assertEqual([e.text for e in overflow], ["second request"])
            await self.app.process_update(upd(self.app.bot, 3012, "second request, fixed", edited=True, mid=631))
            await drain(self.adapter)
            self.assertEqual([e.text for e in overflow], ["second request, fixed"])
            self.assertEqual(self.adapter._pending_messages[key].text, "first request")
            self.assertEqual(runner.calls, 2)

    async def test_edit_of_a_merged_message_waiting_for_the_next_turn_travels_with_it(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3020, "book the 5pm table", mid=640))
            await self.app.process_update(upd(self.app.bot, 3021, "and invite Sam", mid=641))
            await drain(self.adapter)
            queued = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3022, "book the 6pm table", edited=True, mid=640))
            await drain(self.adapter)
            self.assert_in_order(queued.text, "book the 5pm table", "and invite Sam", MARK)
            self.assertIn("Earlier wording (void): «book the 5pm table»" + corrected("book the 6pm table"), queued.text)
            self.assertTrue(queued.allow_gateway_control, "the user's own messages keep gateway control")
            self.assertEqual(runner.calls, 1)

    async def test_queue_mode_edit_of_a_waiting_message_replaces_it_there(self):
        with self.busy(mode="queue") as key:
            await self.app.process_update(upd(self.app.bot, 3030, "first ask", mid=650))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3031, "first ask, corrected", edited=True, mid=650))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3032, "my own follow-up", mid=651))
            await drain(self.adapter)
            await self.adapter._flush_text_debounce_now(key)
            self.assertEqual(self.adapter._pending_messages[key].text, "first ask, corrected\nmy own follow-up")
            self.assertEqual(self.delivered, [])

    async def test_queue_mode_follow_up_after_an_edit_reads_as_a_separate_message(self):
        with self.busy(mode="queue") as key:
            await self.app.process_update(upd(self.app.bot, 3040, "edited while busy", edited=True, mid=470))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3041, "my own follow-up", mid=495))
            await drain(self.adapter)
            await self.adapter._flush_text_debounce_now(key)
            pending = self.adapter._pending_messages[key]
            self.assertTrue(pending.text.startswith(MARK))
            self.assertTrue(pending.text.endswith(corrected("edited while busy") + "\nmy own follow-up"),
                            "Hermes merges busy follow-ups in queue mode; the end line keeps them apart")

    # -- what Hermes does with queued text when the running turn ends (run_turn.py's drain) --

    async def test_edit_into_slash_text_of_a_queued_message_survives_the_drain(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3200, "hello there", mid=820))
            await drain(self.adapter)
            queued = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3201, "/status", edited=True, command=True, mid=820))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 1)
            event, pending = await runner.next_turn(key, queued.source)
            self.assertIs(event, queued, "the corrected message reaches the next turn")
            self.assertTrue(pending.startswith("hello there\n" + MARK), pending)
            self.assertTrue(pending.endswith(corrected("/status")))

    async def test_queue_mode_edit_into_slash_text_and_a_follow_up_survive_the_drain(self):
        runner = BusyRunner(self.adapter)
        with self.busy(mode="queue") as key:
            self.adapter.gateway_runner = runner
            await self.app.process_update(upd(self.app.bot, 3210, "first ask", mid=830))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3211, "/model gpt-5", edited=True, command=True, mid=830))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3212, "and please also check the disk", mid=831))
            await drain(self.adapter)
            await self.adapter._flush_text_debounce_now(key)
            slot = self.adapter._pending_messages[key]
            _event, pending = await runner.next_turn(key, slot.source)
            self.assertIsNotNone(pending, "neither the correction nor the follow-up may be dropped")
            self.assert_in_order(pending, "first ask", MARK, "/model gpt-5", END, "and please also check the disk")

    async def test_rapid_edit_into_slash_text_while_busy_survives_the_drain(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3220, "first ask", mid=840))
            await self.app.process_update(upd(self.app.bot, 3221, "/status", edited=True, command=True, mid=840))
            await self.app.process_update(upd(self.app.bot, 3222, "and please also check the disk", mid=841))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            _event, pending = await runner.next_turn(key, slot.source)
            self.assertIsNotNone(pending)
            self.assert_in_order(pending, "first ask", MARK, "/status", END, "and please also check the disk")

    # -- a message whose text Hermes copied into another waiting event --

    async def test_edit_of_a_question_merged_into_a_queued_photo_travels_with_it(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=850)
            await self.app.process_update(upd(self.app.bot, 3231, "what is in this picture", mid=851))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], photo, "Hermes merged the question into the photo")
            self.assertEqual(photo.text, "what is in this picture")
            calls = runner.calls
            await self.app.process_update(upd(self.app.bot, 3232, "what is in these two pictures", edited=True, mid=851))
            await drain(self.adapter)
            self.assertEqual(runner.calls, calls, "no separate notice overtakes the photo it belongs to")
            self.assertEqual(list(runner._overflow_queue(key) or ()), [])
            self.assert_in_order(photo.text, "what is in this picture", MARK)
            self.assertIn("Earlier wording (void): «what is in this picture»"
                          + corrected("what is in these two pictures"), photo.text)
            self.assertEqual(self.delivered, [])

    async def test_edit_of_a_follow_up_merged_into_the_next_turn_slot_travels_with_it(self):
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3240, "book the table", mid=860))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3241, "for four people", mid=861))
            await drain(self.adapter)
            self.assertEqual(slot.text, "book the table\nfor four people", "Hermes copied the text into the slot")
            await self.app.process_update(upd(self.app.bot, 3242, "for five people", edited=True, mid=861))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 2, "the correction joins the slot instead of taking the busy lane")
            self.assert_in_order(slot.text, "book the table", "for four people", MARK)
            self.assertIn("Earlier wording (void): «for four people»" + corrected("for five people"), slot.text)

    async def test_a_later_message_with_the_same_words_never_takes_the_notice(self):
        await self.app.process_update(upd(self.app.bot, 3245, "yes", mid=865))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3246, "yes", mid=866))    # a new answer, waiting
            await drain(self.adapter)
            waiting = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3247, "no", edited=True, mid=865))
            await drain(self.adapter)
            self.assertEqual(waiting.text, "yes", "the later message is not the one that was edited")
            self.assertEqual(runner.calls, 2)
            [notice] = runner._overflow_queue(key)
            self.assertTrue(notice.text.endswith(corrected("no")))

    async def test_a_second_edit_while_the_first_notice_waits_joins_it(self):
        await self.app.process_update(upd(self.app.bot, 3250, "draft one", mid=870))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3251, "draft two", edited=True, mid=870))
            await drain(self.adapter)
            notice = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3252, "draft three", edited=True, mid=870))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 1)
            self.assert_in_order(notice.text, corrected("draft two"), "Earlier wording (void): «draft two»",
                                 corrected("draft three"))

    # -- cancellation, topic recovery, carriers --

    async def test_a_cancel_after_hermes_took_the_pending_batch_does_not_hold_it_again(self):
        await self.app.process_update(upd(self.app.bot, 3260, "earlier message", mid=880))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 3261, "pending message", mid=881))

        async def taken_then_cancelled(event):
            event._gateway_accepted = True          # Hermes stored it, then the task was cancelled
            raise asyncio.CancelledError()

        self.mp.setattr(self.adapter, "_dispatch_text_batch", taken_then_cancelled)
        update = upd(self.app.bot, 3262, "earlier message, fixed", edited=True, mid=880)
        with self.assertRaises(asyncio.CancelledError):
            await main_state(self.app).current.handle_edit(update, None, self.adapter)
        held = list(self.adapter._held_inbound_events)
        self.assertEqual(len(held), 1, "the batch Hermes took is not held a second time; the edit is held")
        self.assertTrue(held[0].text.endswith(corrected("earlier message, fixed")))

    async def test_a_cancel_while_the_pending_batch_goes_first_holds_both_in_order(self):
        await self.app.process_update(upd(self.app.bot, 3265, "earlier message", mid=885))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 3266, "pending message", mid=886))

        async def cancelled(event):
            raise asyncio.CancelledError()

        self.mp.setattr(self.adapter, "_dispatch_text_batch", cancelled)
        update = upd(self.app.bot, 3267, "earlier message, fixed", edited=True, mid=885)
        with self.assertRaises(asyncio.CancelledError):
            await main_state(self.app).current.handle_edit(update, None, self.adapter)
        held = list(self.adapter._held_inbound_events)
        self.assertEqual(len(held), 2, "neither the pending batch nor the edit is lost")
        self.assertEqual(held[0].text, "pending message")
        self.assertTrue(held[1].text.endswith(corrected("earlier message, fixed")))

    async def test_topic_recovery_applies_before_the_runner_queue_is_searched(self):
        def recover(source):
            return None if source.thread_id else "77"     # a lobby message recovered into topic 77

        self.adapter._topic_recovery_fn = recover
        try:
            probe = self.adapter._build_message_event(upd(self.app.bot, 0, "x").message, T.MessageType.TEXT)
            self.adapter._apply_topic_recovery(probe)
            self.assertEqual(probe.source.thread_id, "77")
            runner = BusyRunner(self.adapter)
            with self.busy(runner, key=self.adapter._event_session_key(probe)) as key:
                await self.app.process_update(upd(self.app.bot, 3271, "first request", mid=891))
                await drain(self.adapter)
                await self.app.process_update(upd(self.app.bot, 3272, "second request", mid=892))
                await drain(self.adapter)
                self.assertEqual([e.text for e in runner._overflow_queue(key)], ["second request"])
                await self.app.process_update(upd(self.app.bot, 3273, "second request, fixed", edited=True, mid=892))
                await drain(self.adapter)
                self.assertEqual([e.text for e in runner._overflow_queue(key)], ["second request, fixed"])
                self.assertEqual(runner.calls, 2)
        finally:
            del self.adapter._topic_recovery_fn

    async def test_a_command_never_takes_a_waiting_message_as_its_carrier(self):
        await self.app.process_update(upd(self.app.bot, 3280, "check this\n/status", mid=900))   # one message
        await self.app.process_update(upd(self.app.bot, 3281, "/status", command=True, mid=901))
        await self.app.process_update(upd(self.app.bot, 3282, "/status verbose", edited=True, command=True, mid=901))
        await drain(self.adapter)
        texts = [e.text for e in self.delivered]
        self.assertIn("check this\n/status", texts, "Hermes never batches commands: that batch is not its carrier")
        self.assertEqual(len(texts), 3)
        self.assertTrue(texts[2].startswith(MARK))

    async def test_an_edited_command_never_joins_an_older_queued_message_that_holds_its_words(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3370, "check this\n/status", mid=980))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            self.assertEqual(slot.text, "check this\n/status")
            await self.app.process_update(upd(self.app.bot, 3371, "/status", command=True, mid=981))
            await drain(self.adapter)                       # a command runs mid-turn, it is never queued
            self.assertIs(self.adapter._pending_messages[key], slot)
            await self.app.process_update(upd(self.app.bot, 3372, "/status verbose", edited=True, command=True,
                                              mid=981))
            await drain(self.adapter)
            self.assertEqual(slot.text, "check this\n/status", "the queued message is another message")
            [notice] = runner._overflow_queue(key)
            self.assertTrue(notice.text.startswith(MARK))
            self.assertIn(corrected("/status verbose"), notice.text)

    async def test_an_edited_command_merged_into_a_queued_photo_gets_a_notice_through_the_busy_lane(self):
        # A message never seen waiting (a command: Hermes batches one only near the split limit, and the
        # recorder gives it no carrier) is never searched for in the slot, even where the runner's merge
        # copied its words into a queued photo (README, Known limits).
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=2050)
            long_command = "/notacommand" + " word" * 800
            self.assertGreaterEqual(len(long_command), self.adapter._SPLIT_THRESHOLD)
            await self.app.process_update(upd(self.app.bot, 5050, long_command, command=True, mid=2051))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], photo)
            self.assertEqual(photo.text, long_command, "the runner merged the command into the photo")
            self.assertEqual(runner.calls, 1)
            await self.app.process_update(upd(self.app.bot, 5051, "/notacommand fixed", edited=True, command=True,
                                              mid=2051))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 2, "the notice took the busy lane as a separate message")
            self.assert_in_order(photo.text, long_command, MARK, corrected("/notacommand fixed"))

    async def test_an_edited_command_merged_after_a_queued_photos_caption_gets_a_notice_through_the_busy_lane(self):
        # The same with a caption on the photo. The merged text then starts with the caption, so it doesn't read as a
        # command, and the refusal of a carrier that does (after the search) lets it through: only the rule that a
        # message never seen waiting is never searched for keeps the notice off the photo.
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=5260)
            photo.text = "a red car"
            long_command = "/notacommand" + " word" * 800
            await self.app.process_update(upd(self.app.bot, 8260, long_command, command=True, mid=5261))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], photo)
            self.assertEqual(photo.text, "a red car\n\n" + long_command, "merged after the caption (_merge_caption)")
            self.assertEqual(runner.calls, 1)
            await self.app.process_update(upd(self.app.bot, 8261, "/notacommand fixed", edited=True, command=True,
                                              mid=5261))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 2, "the notice took the busy lane as a separate message")
            self.assert_in_order(photo.text, "a red car", long_command, MARK, corrected("/notacommand fixed"))

    async def test_a_held_message_never_takes_a_batch_that_ends_in_its_words(self):
        await self.app.process_update(upd(self.app.bot, 3285, "say hello", mid=905))
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)
        await self.app.process_update(upd(self.app.bot, 3286, "hello", mid=906))      # held, never batched
        await self.app.process_update(upd(self.app.bot, 3287, "hello there", edited=True, mid=906))
        await self.teardown_and_reconnect()
        texts = [e.text for e in self.delivered]
        self.assertIn("say hello", texts, "an unrelated batch that only ends in the same words is untouched")
        self.assertEqual(len(texts), 3)
        self.assertTrue(texts[-1].startswith(MARK))

    # -- when the plugin must step aside --

    async def test_a_removed_plugin_directory_hands_edits_back_at_once(self):
        manifest = INSTALLED.with_name("plugin.yaml")
        hidden = manifest.with_name("plugin.yaml.removed")
        manifest.rename(hidden)
        try:
            await self.app.process_update(upd(self.app.bot, 3290, "after removal", edited=True, mid=910))
            await drain(self.adapter)
        finally:
            hidden.rename(manifest)
        self.assertEqual([e.text for e in self.delivered], ["after removal"])
        self.assertTrue(self.delivered[0].allow_gateway_control, "Hermes' own path, untouched")

    async def test_a_hermes_that_differs_from_the_verified_one_leaves_edits_to_hermes(self):
        pin = INSTALLED.with_name("verified_hermes.json")
        saved = pin.read_text()
        data = json.loads(saved)
        first = sorted(data["files"])[0]
        data["files"][first] = "0" * 64
        try:
            pin.write_text(json.dumps(data))
            with self.assertLogs(LOGGER, "INFO") as logs:
                await self.reload()
            joined = "\n".join(logs.output)
            self.assertIn("differs from the version", joined)
            self.assertIn(first, joined)
            self.assertRegex(joined, r"loaded \(pid \d+, inactive: ")
            await self.app.process_update(upd(self.app.bot, 3295, "unverified", edited=True, mid=915))
            await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["unverified"])
            self.assertTrue(self.delivered[0].allow_gateway_control, "Hermes' own path, untouched")
        finally:
            pin.write_text(saved)
            await self.reload()
        await self.app.process_update(upd(self.app.bot, 3296, "verified again", edited=True, mid=916))
        await drain(self.adapter)
        self.assertTrue(self.delivered[-1].text.startswith(MARK))

    async def test_the_load_line_reports_an_active_plugin(self):
        with self.assertLogs(LOGGER, "INFO") as logs:
            await self.reload()
        self.assertRegex("\n".join(logs.output), r"telegram-edit-replacement loaded \(pid \d+, active\)")

    async def test_a_corrected_message_merged_into_the_slot_still_takes_a_later_edit(self):
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3310, "book the table", mid=930))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3311, "for four people", mid=931))
            await self.app.process_update(upd(self.app.bot, 3312, "for five people", edited=True, mid=931))
            await drain(self.adapter)                 # replaced in its batch, then copied into the slot
            self.assertEqual(slot.text, "book the table\nfor five people")
            await self.app.process_update(upd(self.app.bot, 3313, "for six people", edited=True, mid=931))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 2)
            self.assertIn("Earlier wording (void): «for five people»" + corrected("for six people"), slot.text)

    async def test_a_waiting_event_without_a_telegram_message_id_is_never_taken(self):
        await self.app.process_update(upd(self.app.bot, 3320, "status please", mid=940))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            from gateway.platforms.event import MessageEvent
            source = self.adapter._build_message_event(upd(self.app.bot, 0, "x").message, T.MessageType.TEXT).source
            internal = MessageEvent(text="status please", source=source)    # e.g. a leftover /steer
            runner._queue_or_replace_pending_event(key, internal)
            with self.assertNoLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 3321, "status now", edited=True, mid=940))
                await drain(self.adapter)
            self.assertEqual(internal.text, "status please")
            [notice] = runner._overflow_queue(key)
            self.assertTrue(notice.text.startswith(MARK))

    async def test_a_record_kept_by_an_older_version_still_gets_a_notice(self):
        await self.app.process_update(upd(self.app.bot, 3330, "old record", mid=950))
        await drain(self.adapter)
        cache = main_state(self.app).current.cache
        seen = cache.get(88, 950)
        old = types.SimpleNamespace(wording=seen.wording, fingerprint=seen.fingerprint, holder=seen.holder,
                                    holder_digest=seen.holder_digest, carrier=seen.carrier)   # no length
        main_state(self.app).wording[("88", "950")] = old
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            self.queue_photo(runner, key, mid=945)        # an older event waits in the slot
            with self.assertNoLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 3331, "new record", edited=True, mid=950))
                await drain(self.adapter)
            [notice] = runner._overflow_queue(key)
            self.assertTrue(notice.text.startswith(MARK))

    async def test_adding_or_dropping_the_bots_handle_changes_nothing_the_agent_gets(self):
        self.adapter._bot_username_observed = "edit_test_bot"
        try:
            await self.app.process_update(upd(self.app.bot, 3340, "@edit_test_bot what time is it", mid=960))
            await drain(self.adapter)
            self.assertEqual(self.delivered[0].text, "what time is it", "Hermes drops its own handle")
            await self.app.process_update(upd(self.app.bot, 3341, "what time is it", edited=True, mid=960))
            await drain(self.adapter)
            self.assertEqual(len(self.delivered), 1, "the agent would get the same text: nothing to correct")
        finally:
            self.adapter.__dict__.pop("_bot_username_observed", None)

    async def test_a_second_edit_finds_its_notice_where_hermes_merged_it(self):
        await self.app.process_update(upd(self.app.bot, 3350, "draft one", mid=971))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=969)       # older, still waiting
            await self.app.process_update(upd(self.app.bot, 3351, "draft two", edited=True, mid=971))
            await drain(self.adapter)
            self.assertTrue(photo.text.endswith(corrected("draft two")), "Hermes merged the notice into the slot")
            await self.app.process_update(upd(self.app.bot, 3352, "draft three", edited=True, mid=971))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 1, "the second correction joins the first where it waits")
            self.assertIn("Earlier wording (void): «draft two»" + corrected("draft three"), photo.text)

    async def test_a_slot_that_holds_the_words_only_inside_a_line_is_not_taken(self):
        await self.app.process_update(upd(self.app.bot, 3360, "book it", mid=976))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=975)
            photo.text = "please book it"                         # another message, older, same words inside
            await self.app.process_update(upd(self.app.bot, 3361, "cancel it", edited=True, mid=976))
            await drain(self.adapter)
            self.assertEqual(photo.text, "please book it")
            self.assertEqual(runner.calls, 1)

    # -- a message steered into the running turn, while older messages wait for the next one --

    async def test_a_steered_message_never_takes_an_older_queued_message_that_holds_its_words(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3380, "delete the old backups?\nyes", mid=990))
            await drain(self.adapter)                     # queued while the turn's agent is being set up
            slot = self.adapter._pending_messages[key]
            runner.steering = True                        # the agent is live: plain text goes into the turn
            await self.app.process_update(upd(self.app.bot, 3381, "yes", mid=991))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.steered], ["yes"])
            await self.app.process_update(upd(self.app.bot, 3382, "no", edited=True, mid=991))
            await drain(self.adapter)
            self.assertEqual(slot.text, "delete the old backups?\nyes", "another message that holds the words")
            self.assertEqual(len(runner.steered), 2, "the correction reaches the running turn now")
            self.assertTrue(runner.steered[1].text.startswith(MARK))
            self.assertTrue(runner.steered[1].text.endswith(corrected("no")))

    async def test_a_message_that_waited_behind_the_slot_is_not_taken_after_moving_up(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3384, "first question", mid=994))
            await drain(self.adapter)
            head = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3385, "delete the old backups?\nyes", mid=995))
            await drain(self.adapter)
            [behind] = runner._overflow_queue(key)        # waits behind the next turn's input
            runner.steering = True
            await self.app.process_update(upd(self.app.bot, 3386, "yes", mid=996))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.steered], ["yes"])
            event, _pending = await runner.next_turn(key, head.source)     # that turn ends, the next starts
            self.assertIs(event, head)
            self.assertIs(self.adapter._pending_messages.get(key), behind, "the message behind moved up")
            self.adapter._active_sessions[key] = asyncio.Event()           # the next turn runs
            await self.app.process_update(upd(self.app.bot, 3387, "no", edited=True, mid=996))
            await drain(self.adapter)
            self.assertEqual(behind.text, "delete the old backups?\nyes")
            self.assertEqual(len(runner.steered), 2, "the correction reaches the running turn now")
            self.assertTrue(runner.steered[1].text.endswith(corrected("no")))

    async def test_a_message_corrected_in_place_then_steered_never_takes_an_older_message_with_its_words(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3410, "delete the old backups?\nno", mid=1040))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            runner.steering = True
            await self.app.process_update(upd(self.app.bot, 3411, "yes", mid=1041))              # in its batch
            await self.app.process_update(upd(self.app.bot, 3412, "no", edited=True, mid=1041))  # replaced there
            await drain(self.adapter)                                                           # then steered
            self.assertEqual([e.text for e in runner.steered], ["no"])
            await self.app.process_update(upd(self.app.bot, 3413, "no, keep them", edited=True, mid=1041))
            await drain(self.adapter)
            self.assertEqual(slot.text, "delete the old backups?\nno", "that line was there before the correction")
            self.assertEqual(len(runner.steered), 2)
            self.assertTrue(runner.steered[1].text.endswith(corrected("no, keep them")))

    async def test_after_a_steered_notice_a_later_edit_never_takes_an_older_message_with_its_words(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3415, "which one?\nfinal", mid=1044))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            runner.steering = True
            await self.app.process_update(upd(self.app.bot, 3416, "draft", mid=1045))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3417, "final", edited=True, mid=1045))
            await drain(self.adapter)
            await self.app.process_update(upd(self.app.bot, 3418, "final!", edited=True, mid=1045))
            await drain(self.adapter)
            self.assertEqual(slot.text, "which one?\nfinal", "that line was there before the first notice")
            self.assertEqual(len(runner.steered), 3)
            for notice, text in zip(runner.steered[1:], ("final", "final!")):
                self.assertTrue(notice.text.endswith(corrected(text)))

    async def test_known_limit_a_cancel_right_after_a_steer_holds_the_notice_again(self):
        """Hermes marks a steered event accepted nowhere (run_busy.py sets _gateway_accepted only for the
        slot, FIFO, photo and a started turn), so a cancel that lands right after the steer holds the
        notice again although the running turn has it. Hermes' own flush does the same for every message
        (adapter._flush_buffered re-holds any popped event a cancel interrupts). README: Known limits."""
        await self.app.process_update(upd(self.app.bot, 3405, "draft", mid=1020))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter)
        runner.steering = runner.cancel_after_steer = True
        with self.busy(runner) as key:
            turn = runner._session_state(key).turn
            with self.assertRaises(asyncio.CancelledError):
                await self.app.process_update(upd(self.app.bot, 3406, "final", edited=True, mid=1020))
        [steered] = runner.steered
        self.assertTrue(steered.text.endswith(corrected("final")))
        self.assertIn(steered, list(self.adapter._held_inbound_events))
        self.assertEqual(turn.busy_ack_ts, 0.0, "the cancel still puts the turn's acknowledgement time back")

    # -- teardown --

    async def test_a_message_held_in_teardown_never_takes_the_batch_of_earlier_messages(self):
        await self.app.process_update(upd(self.app.bot, 3390, "first", mid=1000))
        await self.app.process_update(upd(self.app.bot, 3391, "hello", mid=1001))      # one batch: first\nhello
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)                # teardown starts
        await self.app.process_update(upd(self.app.bot, 3392, "hello", mid=1002))      # held, never batched
        await self.app.process_update(upd(self.app.bot, 3393, "hello there", edited=True, mid=1002))
        await self.teardown_and_reconnect()
        texts = [e.text for e in self.delivered]
        self.assertEqual(sorted(texts[:2]), ["first\nhello", "hello"], "both untouched, the void one included")
        self.assertEqual(len(texts), 3)
        self.assertTrue(texts[2].startswith(MARK), "the notice after the void original")

    # -- corrections that read as commands --

    async def test_an_edit_into_a_gateway_restart_phrase_never_replaces_the_waiting_text(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3395, "check the logs", mid=1010))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(upd(self.app.bot, 3396, "Restart the gateway.", edited=True, mid=1010))
            await drain(self.adapter)
            self.assertTrue(slot.text.startswith("check the logs\n" + MARK), slot.text)
            self.assertIn(NOT_RUN, slot.text)
            self.assertTrue(slot.text.endswith(corrected("Restart the gateway.")))

    async def test_a_correction_that_starts_with_a_path_is_conversation_not_a_command(self):
        # Hermes gives '/tmp/...' to the agent as text (MessageEvent.get_command rejects a name with '/').
        await self.app.process_update(upd(self.app.bot, 3420, "where is the list?", mid=1050))
        await drain(self.adapter)
        text = "/tmp/notes.md has the list"
        await self.app.process_update(upd(self.app.bot, 3421, text, edited=True, mid=1050))
        await drain(self.adapter)
        [notice] = [e.text for e in self.delivered[1:]]
        self.assertTrue(notice.endswith(corrected(text)))
        self.assertNotIn(NOT_RUN, notice)

    async def test_a_waiting_message_corrected_into_a_path_gets_a_notice_that_never_says_not_run(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3425, "where is the list?", mid=1052))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            text = "/tmp/notes.md has the list"
            await self.app.process_update(upd(self.app.bot, 3426, text, edited=True, mid=1052))
            await drain(self.adapter)
            self.assertTrue(slot.text.startswith("where is the list?\n" + MARK), "a leading slash is never put in place")
            self.assertTrue(slot.text.endswith(corrected(text)))
            self.assertNotIn(NOT_RUN, slot.text)

    async def test_a_correction_reads_as_a_command_exactly_when_hermes_would_run_it(self):
        from gateway.platforms.base import coerce_plaintext_gateway_command
        update = upd(self.app.bot, 3428, "anything", edited=True, mid=1054)
        event = await self.adapter._build_triggered_event(update.edited_message, update, T.MessageType.TEXT)
        reading = sys.modules[type(main_state(self.app).current).__module__]._command_reading
        cases = {"/status": (True, True), "/model@example_bot some-model": (True, True), "  /help": (True, True),
                 "/nosuchcommand x": (True, True),            # Hermes answers 'Unknown command', never the agent
                 "/tmp/notes.md has the list": (True, False), "/": (True, False),
                 "restart hermes": (False, True), "Please restart the gateway!": (False, True),
                 "restart the gateway later": (False, False), "the /status page": (False, False)}
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(reading(event, text), expected)
                probe = dataclasses.replace(event, text=text, allow_gateway_control=True)
                hermes_runs = bool(probe.get_command())     # run_inbound.py: 'if not command' is conversation
                if not hermes_runs:
                    coerce_plaintext_gateway_command(probe)
                    hermes_runs = probe.text != text
                self.assertEqual(expected[1], hermes_runs, "Hermes' own reading")
        self.assertEqual(event.text, "anything", "the edit's own event is never changed")
        no_control = dataclasses.replace(event, allow_gateway_control=False)
        self.assertEqual(reading(no_control, "/status"), (True, True), "however the edit's event is marked")
        self.assertEqual(reading(no_control, "restart hermes"), (False, True))
        self.assertFalse(no_control.allow_gateway_control, "the edit's own event is never changed")
        self.assertEqual(no_control.text, "anything")

    async def test_a_correction_reads_as_a_command_exactly_when_hermes_inbound_path_runs_one(self):
        """Each case sent as a fresh owner message to an idle chat, through the real path that decides command
        or conversation: PTB and the Telegram adapter (batching, base.handle_message's plaintext /restart
        coercion), then the runner's real idle dispatch (run_inbound.py _hm_dispatch_idle_commands: command
        resolution, built-in handlers, quick and plugin commands, skill rewrite or the 'Unknown command'
        reply). Stubbed: what a command does once dispatched (its handler, recorded), the slash access
        policy, hooks and the executor hop. Not driven: _handle_message's admission, estop gate and pending
        prompt intercepts, which never apply to an idle owner chat without a pending prompt."""
        import contextlib as _contextlib
        from gateway.run import GatewayRunner
        reading = sys.modules[type(main_state(self.app).current).__module__]._command_reading
        ran = []

        def handler(name):
            async def run(event, *rest):
                ran.append(name)
                return (True, f"ran {name}") if rest else f"ran {name}"
            return run

        async def no_hooks(name, context):
            return []

        async def inline(func, *args):
            return func(*args)

        runner = object.__new__(GatewayRunner)
        runner.config = {}
        runner._draining = False
        runner.hooks = types.SimpleNamespace(emit_collect=no_hooks)
        runner._command_handler_table = lambda names: {name: handler(name) for name in names}
        for name in GatewayRunner._HM_CANONICAL_COMMANDS:
            setattr(runner, f"_hm_cmd_{name}", handler(name))
        runner._async_profile_scope_for_source = lambda source: _contextlib.nullcontext()
        runner._check_slash_access = lambda source, command: None
        runner._run_in_executor_with_context = inline
        cases = [("/status", True, True), ("/status verbose", True, True),
                 ("/tmp/notes.md has the list", False, False), ("/", False, False),
                 ("/notacommand", True, True), ("restart gateway", False, True),
                 ("please restart the hermes gateway", False, True), ("Restart Hermes!", False, True),
                 ("restart hermes now", False, False), ("hello there", False, False)]
        for i, (text, command, expected) in enumerate(cases):
            with self.subTest(text=text):
                before = len(self.delivered)
                ran.clear()
                await self.app.process_update(upd(self.app.bot, 5060 + i, text, mid=2060 + i, command=command))
                await drain(self.adapter)
                [event] = self.delivered[before:]
                arrived = event.text
                handled, result = await GatewayRunner._hm_dispatch_idle_commands(runner, event, event.source, "k")
                hermes_runs = bool(handled or ran or event.text != arrived)
                self.assertEqual(hermes_runs, expected, (handled, result, ran, arrived, event.text))
                probe = self.adapter._build_message_event(upd(self.app.bot, 0, text).message, T.MessageType.TEXT)
                self.assertEqual(reading(probe, text)[1], hermes_runs)

    async def test_a_text_put_in_place_never_counts_as_appended_for_messages_recorded_before(self):
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 3430, "delete the old backups?", mid=1060))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]               # waits alone for the next turn
            runner.steering = True                                   # the agent is live now
            await self.app.process_update(upd(self.app.bot, 3431, "yes", mid=1061))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.steered], ["yes"])
            await self.app.process_update(upd(self.app.bot, 3432, "which old backups should go?\nyes",
                                              edited=True, mid=1060))
            await drain(self.adapter)
            self.assertEqual(slot.text, "which old backups should go?\nyes", "put in place")
            await self.app.process_update(upd(self.app.bot, 3433, "no", edited=True, mid=1061))
            await drain(self.adapter)
            self.assertEqual(slot.text, "which old backups should go?\nyes", "the slot never held message 1061")
            self.assertEqual(len(runner.steered), 2)
            self.assertTrue(runner.steered[1].text.endswith(corrected("no")))

    async def test_the_running_commit_is_hermes_own_code_identity(self):
        from hermes_cli.version_info import get_code_identity
        pkg = sys.modules[type(main_state(self.app).current).__module__]
        running = pkg._running_commit()
        self.assertRegex(running or "", r"^[0-9a-f]{40}$")
        self.assertEqual(running, get_code_identity()["sha"])
        pin = json.loads((INSTALLED.parent / "verified_hermes.json").read_text())
        self.assertEqual(pin["hermes_commit"], running, "the harness pin names the Hermes these tests run")

    async def test_a_notice_whose_corrected_text_is_a_command_says_it_was_not_run(self):
        await self.app.process_update(upd(self.app.bot, 3400, "status of the backups", mid=1015))
        await drain(self.adapter)
        edits = (("please restart hermes", False, True), ("/status", True, True),
                 ("Restart the gateway!", False, True), ("restart the gateway later", False, False),
                 ("status of the old backups", False, False))
        for n, (text, command, _not_run) in enumerate(edits):
            await self.app.process_update(upd(self.app.bot, 3401 + n, text, edited=True, command=command, mid=1015))
            await drain(self.adapter)
        notices = [e.text for e in self.delivered[1:]]
        self.assertEqual(len(notices), len(edits))
        for (text, _command, not_run), notice in zip(edits, notices):
            with self.subTest(text=text):
                self.assertTrue(notice.endswith(corrected(text)))
                self.assertEqual(NOT_RUN in notice, not_run)

    def test_these_tests_run_the_hermes_that_pin_hermes_pins(self):
        import gateway
        import hermes_cli
        for module in (hermes_cli, gateway, T):
            with self.subTest(module=module.__name__):
                self.assertTrue(Path(module.__file__).resolve().is_relative_to(REPO.resolve()), module.__file__)

    async def test_formatting_only_edit_of_a_long_message_is_not_delivered_again(self):
        long_text = ("word " * 300).strip()
        await self.app.process_update(upd(self.app.bot, 3050, long_text, mid=700))
        await drain(self.adapter)
        bold = [{"type": "bold", "offset": 0, "length": 4}]
        await self.app.process_update(upd(self.app.bot, 3051, long_text, edited=True, mid=700, entities=bold))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)

    async def test_formatting_only_edit_of_a_merged_message_with_a_hidden_link_is_not_delivered_again(self):
        link = [{"type": "text_link", "offset": 4, "length": 4, "url": "https://example.com/docs"}]
        await self.app.process_update(upd(self.app.bot, 3060, "first", mid=710))
        await self.app.process_update(upd(self.app.bot, 3061, "see docs now", mid=711, entities=link))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)
        bold = link + [{"type": "bold", "offset": 0, "length": 3}]
        await self.app.process_update(upd(self.app.bot, 3062, "see docs now", edited=True, mid=711, entities=bold))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)

    # -- a message that replies to media: Hermes appends a note naming the cached media to its text --

    def replied_media(self):
        """Stub the download behind adapter._cache_replied_media: every build of the turn caches the
        replied-to photo again, under a new name, as Hermes' media cache does. Returns the calls."""
        calls = []

        async def download(msg, what):
            calls.append(what)
            return "ok", types.SimpleNamespace(kind="image", display_name="photo.jpg", media_type="image/jpeg",
                                               path=f"/nonexistent/replied-{len(calls)}.jpg")

        self.mp.setattr(self.adapter, "_download_observed_media", download)
        return calls

    async def test_formatting_only_edit_of_a_message_replying_to_a_photo_is_not_delivered_again(self):
        calls = self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 4400, "what is this", mid=1880))
        await drain(self.adapter)
        [first] = self.delivered
        self.assertTrue(first.text.startswith("what is this\n\n[Replied-to image 'photo.jpg'"), first.text)
        bold = [{"type": "bold", "offset": 0, "length": 4}]
        await self.app.process_update(reply_upd(self.app.bot, 4401, "what is this", edited=True, mid=1880,
                                                entities=bold))
        await drain(self.adapter)
        self.assertEqual(len(calls), 2, "Hermes built the edit's turn, note included")
        self.assertEqual(self.delivered, [first], "the words are unchanged: nothing is delivered")

    async def test_formatting_only_edit_of_a_waiting_message_replying_to_a_photo_changes_nothing(self):
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 4410, "describe it", mid=1881))
        [batch] = self.adapter._pending_text_batches.values()
        before = batch.text
        self.assertIn("[Replied-to image", before)
        bold = [{"type": "bold", "offset": 0, "length": 8}]
        await self.app.process_update(reply_upd(self.app.bot, 4411, "describe it", edited=True, mid=1881,
                                                entities=bold))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], [before])

    async def test_real_edit_of_a_message_replying_to_a_photo_quotes_the_words_the_user_wrote(self):
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 4420, "what is this", mid=1882))
        await drain(self.adapter)
        await self.app.process_update(reply_upd(self.app.bot, 4421, "who made this", edited=True, mid=1882))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        notice = self.delivered[1].text
        self.assertTrue(notice.startswith(MARK), notice)
        self.assertIn("Earlier wording (void): «what is this»\nCorrected text:\nwho made this\n\n[Replied-to image",
                      notice)

    async def test_second_edit_of_a_delivered_message_replying_to_a_photo_quotes_the_corrected_words(self):
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 4430, "what is this", mid=1883))
        await drain(self.adapter)
        await self.app.process_update(reply_upd(self.app.bot, 4431, "who made this", edited=True, mid=1883))
        await drain(self.adapter)
        await self.app.process_update(reply_upd(self.app.bot, 4432, "who painted this", edited=True, mid=1883))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 3)
        self.assertIn("Earlier wording (void): «who made this»" + corrected_start("who painted this"),
                      self.delivered[2].text)

    async def test_second_edit_of_a_waiting_message_replying_to_a_photo_quotes_the_corrected_words(self):
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 4440, "describe it", mid=1884))
        await self.app.process_update(reply_upd(self.app.bot, 4441, "describe the colors", edited=True, mid=1884))
        await self.app.process_update(reply_upd(self.app.bot, 4442, "describe the shapes", edited=True, mid=1884))
        await drain(self.adapter)
        [batch] = self.delivered
        self.assertIn("«describe the colors»" + corrected_start("describe the shapes"), batch.text)
        self.assert_in_order(batch.text, "describe it", "«describe it»" + corrected_start("describe the colors"),
                             "«describe the colors»" + corrected_start("describe the shapes"))

    async def test_an_error_after_the_text_was_replaced_in_place_never_delivers_the_edit_again(self):
        await self.app.process_update(upd(self.app.bot, 4450, "first wording", mid=1885))
        generation = main_state(self.app).current

        def broken_rebase(event):
            raise RuntimeError("simulated defect after the corrected text was put in place")

        self.mp.setattr(generation, "_rebase", broken_rebase)
        with self.assertLogs(LOGGER, "WARNING"):
            await self.app.process_update(upd(self.app.bot, 4451, "second wording", edited=True, mid=1885))
            await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["second wording"])

    async def test_an_edit_back_after_an_error_that_followed_an_in_place_replacement_is_delivered(self):
        # Whatever fails after the corrected text was put in place, the record describes the text in place:
        # editing back to the earlier wording is a change, not "unchanged".
        await self.app.process_update(upd(self.app.bot, 5040, "first wording", mid=2040))
        generation = main_state(self.app).current

        def broken_rebase(event):
            raise RuntimeError("simulated defect after the corrected text was put in place")

        generation.__dict__["_rebase"] = broken_rebase
        try:
            with self.assertLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 5041, "second wording", edited=True, mid=2040))
        finally:
            del generation.__dict__["_rebase"]
        [batch] = self.adapter._pending_text_batches.values()
        self.assertEqual(batch.text, "second wording")
        await self.app.process_update(upd(self.app.bot, 5042, "first wording", edited=True, mid=2040))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["first wording"], "the text in place is the last edit")

    # -- records of an edit hold the user's words, never Hermes' turn with its note about the media --

    async def test_formatting_only_edit_after_a_real_edit_of_a_reply_to_a_photo_is_not_delivered_again(self):
        # The notice's record: its fingerprint decides "unchanged" for the next edit.
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 5000, "what is this", mid=2000))
        await drain(self.adapter)
        await self.app.process_update(reply_upd(self.app.bot, 5001, "who made this", edited=True, mid=2000))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        bold = [{"type": "bold", "offset": 0, "length": 3}]
        await self.app.process_update(reply_upd(self.app.bot, 5002, "who made this", edited=True, mid=2000,
                                                entities=bold))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2, "the words are unchanged: nothing is delivered")

    async def test_formatting_only_edit_after_a_real_edit_of_a_waiting_reply_to_a_photo_changes_nothing(self):
        # The record of a correction that went with the waiting text: its fingerprint decides "unchanged"
        # for the next edit.
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 5010, "describe it", mid=2010))
        await self.app.process_update(reply_upd(self.app.bot, 5011, "describe the colors", edited=True, mid=2010))
        [batch] = self.adapter._pending_text_batches.values()
        corrected_once = batch.text
        self.assert_in_order(corrected_once, "describe it\n\n[Replied-to image", MARK,
                             corrected_start("describe the colors"))
        bold = [{"type": "bold", "offset": 0, "length": 8}]
        await self.app.process_update(reply_upd(self.app.bot, 5012, "describe the colors", edited=True, mid=2010,
                                                entities=bold))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], [corrected_once], "the waiting text is left as it was")

    async def test_a_second_edit_of_a_reply_to_a_photo_joins_its_notice_where_hermes_merged_it(self):
        # The notice's record: its length finds the corrected words where Hermes copied the notice.
        self.replied_media()
        await self.app.process_update(reply_upd(self.app.bot, 5020, "draft one", mid=2021))
        await drain(self.adapter)
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=2020)
            await self.app.process_update(reply_upd(self.app.bot, 5021, "draft two", edited=True, mid=2021))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], photo)
            self.assertIn(corrected_start("draft two"), photo.text, "Hermes merged the notice into the photo")
            self.assertEqual(runner.calls, 1)
            await self.app.process_update(reply_upd(self.app.bot, 5022, "draft three", edited=True, mid=2021))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 1, "the second correction joins the first where Hermes merged it")
            self.assertEqual(list(runner._overflow_queue(key) or ()), [])
            self.assert_in_order(photo.text, corrected_start("draft two"),
                                 "Earlier wording (void): «draft two»" + corrected_start("draft three"))

    async def test_an_edit_of_a_waiting_reply_to_a_photo_then_merged_into_a_queued_photo_finds_it_there(self):
        # The record of a correction that went with the waiting text: its length finds the corrected words
        # where Hermes then copied that text.
        self.replied_media()
        runner = BusyRunner(self.adapter, merge=True)
        with self.busy(runner) as key:
            photo = self.queue_photo(runner, key, mid=2030)
            await self.app.process_update(reply_upd(self.app.bot, 5030, "draft one", mid=2031))
            await self.app.process_update(reply_upd(self.app.bot, 5031, "draft two", edited=True, mid=2031))
            await drain(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], photo)
            self.assert_in_order(photo.text, "draft one\n\n[Replied-to image", MARK, corrected_start("draft two"))
            self.assertEqual(runner.calls, 1)
            await self.app.process_update(reply_upd(self.app.bot, 5032, "draft three", edited=True, mid=2031))
            await drain(self.adapter)
            self.assertEqual(runner.calls, 1, "the correction joins the photo instead of taking the busy lane")
            self.assertEqual(list(runner._overflow_queue(key) or ()), [])
            self.assertIn("Earlier wording (void): «draft two»" + corrected_start("draft three"), photo.text)

    async def test_config_that_stops_parsing_keeps_the_plugin_working_across_a_reload(self):
        try:
            CONFIG.write_text("plugins: {enabled: [telegram-edit-replacement]\n")   # not valid YAML
            bump(CONFIG)
            await self.reload()            # Hermes loads from its last good config: the plugin is loaded
            await self.app.process_update(upd(self.app.bot, 3070, "edit while config broken", edited=True, mid=720))
            await drain(self.adapter)
            self.assertEqual(len(self.delivered), 1)
            self.assertTrue(self.delivered[0].text.startswith(MARK))
        finally:
            await self.restore_enabled()

    async def test_cancel_during_the_edits_own_dispatch_holds_it_for_redispatch(self):
        async def cancelled(event):
            raise asyncio.CancelledError()

        self.mp.setattr(self.adapter, "_dispatch_text_batch", cancelled)
        update = upd(self.app.bot, 3080, "held, not lost", edited=True, mid=730)
        with self.assertRaises(asyncio.CancelledError):
            await main_state(self.app).current.handle_edit(update, None, self.adapter)
        held = list(self.adapter._held_inbound_events)
        self.assertEqual(len(held), 1)
        self.assertTrue(held[0].text.endswith(corrected("held, not lost")))

    async def test_a_failing_edit_is_handed_to_hermes_once(self):
        async def broken_build(msg, update, msg_type):
            raise RuntimeError("simulated adapter change")

        core = []

        async def failing_core(update, context):
            core.append(update.update_id)
            raise RuntimeError("Hermes' handler failed too")

        self.mp.setattr(self.adapter, "_build_triggered_event", broken_build)
        self.mp.setattr(self.adapter, "_handle_text_message", failing_core)
        with self.assertLogs(LOGGER, "WARNING"), contextlib.suppress(RuntimeError):
            await self.app.process_update(upd(self.app.bot, 3090, "once", edited=True, mid=740))
        self.assertEqual(core, [3090])

    async def test_edit_that_only_changes_a_hidden_link_is_delivered(self):
        old = [{"type": "text_link", "offset": 4, "length": 4, "url": "https://example.com/old"}]
        new = [{"type": "text_link", "offset": 4, "length": 4, "url": "https://example.com/new"}]
        await self.app.process_update(upd(self.app.bot, 3110, "see docs now", mid=760, entities=old))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 3111, "see docs now", edited=True, mid=760, entities=new))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2, "the agent gets the link inlined: a new link is a change")
        self.assertIn("example.com/new", self.delivered[1].text)

    async def test_an_error_before_anything_changed_hands_the_edit_to_hermes(self):
        policy = main_state(self.app).current.policy

        def broken(*args, **kwargs):
            raise RuntimeError("simulated defect while building the notice")

        policy.notice = broken
        try:
            with self.assertLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 3120, "plain fallback", edited=True, mid=770))
                await drain(self.adapter)
        finally:
            del policy.notice
        self.assertEqual([e.text for e in self.delivered], ["plain fallback"])
        self.assertTrue(self.delivered[0].allow_gateway_control, "Hermes' own path, untouched")

    async def test_a_command_is_never_taken_as_part_of_a_waiting_batch(self):
        await self.app.process_update(upd(self.app.bot, 3100, "first", mid=750))
        await self.app.process_update(upd(self.app.bot, 3101, "/status", command=True, mid=751))
        await self.app.process_update(upd(self.app.bot, 3102, "/status verbose", edited=True, command=True, mid=751))
        await drain(self.adapter)
        self.assertEqual([e.text.split("\n")[0] for e in self.delivered][:2], ["/status", "first"])
        self.assertEqual(len(self.delivered), 3, "Hermes never batches commands: the batch is not the carrier")
        self.assertTrue(self.delivered[2].text.startswith(MARK))

    async def test_busy_lane_sees_replacement_before_gateway_hook(self):
        key = self.adapter._event_session_key(self.adapter._build_message_event(
            upd(self.app.bot, 299, "dummy").message, T.MessageType.TEXT))
        guard = asyncio.Event()
        self.adapter._active_sessions[key] = guard
        self.adapter._session_tasks[key] = asyncio.current_task()
        got = []
        async def busy(e, sk):
            got.append(e)
            e._gateway_accepted = True
            return True
        self.adapter.set_busy_session_handler(busy)
        try:
            await self.app.process_update(upd(self.app.bot, 300, "changed while busy", edited=True))
            await drain(self.adapter)
            self.assertEqual(len(got), 1)
            self.assertEqual(got[0].message_type, T.MessageType.TEXT, "a plain text event: steerable")
            self.assertFalse(got[0].media_urls)
            self.assertIn("not a repeated or additional request", got[0].text)
            self.assertTrue(got[0].text.endswith(corrected("changed while busy")))
            self.assertFalse(got[0].allow_gateway_control)
            self.assertEqual(len(self.delivered), 0)
        finally:
            self.adapter._session_tasks.pop(key, None)
            self.adapter._active_sessions.pop(key, None)

    async def test_fresh_command_is_unmodified_but_edit_is_not_reexecuted(self):
        await self.app.process_update(upd(self.app.bot, 400, "/status", command=True))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 401, "/status verbose", edited=True, command=True))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].get_command(), "status")
        self.assertIsNone(self.delivered[1].get_command())
        self.assertIn(IGNORE, self.delivered[1].text)

    async def test_other_user_untouched_and_bot_unauthorized_stays_dropped(self):
        await self.app.process_update(upd(self.app.bot, 500, "not owner", edited=True, chat=42))
        await drain(self.adapter)
        self.assertEqual(self.delivered[0].text, "not owner")
        await self.app.process_update(upd(self.app.bot, 501, "unauthorized", edited=True, user=99))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)

    async def test_wiring_does_not_keep_telegram_clients_alive(self):
        import gc
        import weakref
        pkg = load_installed_plugin("policy_gc")
        factory = pkg.make_factory(pkg.shared_state("gc-test"))

        class Native:
            def __init__(self):
                self.added = []
            def add_handler(self, handler, group=0):
                self.added.append((handler, group))

        class Adapter:
            pass

        native, adapter = Native(), Adapter()
        adapter._app, native.adapter = native, adapter  # real adapters and clients reference each other
        factory(native, adapter)
        factory(native, adapter)                         # Hermes may run it again for the same client
        self.assertEqual(sorted(group for _h, group in native.added), [0, 97])
        ref = weakref.ref(native)
        del native, adapter
        gc.collect()
        self.assertIsNone(ref(), "plugin state kept a replaced Telegram client alive")

    async def test_earlier_wording_follows_the_edit_chain(self):
        await self.app.process_update(upd(self.app.bot, 700, "meet at 5pm", mid=480))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 701, "meet at 6pm", edited=True, mid=480))
        await drain(self.adapter)
        await self.app.process_update(upd(self.app.bot, 702, "meet at 7pm", edited=True, mid=480))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 3)
        self.assertIn("Earlier wording (void): «meet at 5pm»" + corrected("meet at 6pm"), self.delivered[1].text)
        self.assertIn("Earlier wording (void): «meet at 6pm»" + corrected("meet at 7pm"), self.delivered[2].text)
        self.assertNotIn("5pm", self.delivered[2].text)

    async def test_internal_error_falls_back_to_untagged_core_delivery(self):
        real = self.adapter._build_triggered_event
        calls = []

        async def flaky(msg, update, msg_type):
            calls.append(update.update_id)
            if len(calls) == 1:
                raise RuntimeError(f"simulated adapter change while handling {msg.text!r}")
            return await real(msg, update, msg_type)

        self.mp.setattr(self.adapter, "_build_triggered_event", flaky)
        with self.assertLogs(LOGGER, "WARNING") as logs:
            await self.app.process_update(upd(self.app.bot, 800, "still delivered", edited=True, mid=490))
            await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["still delivered"])
        self.assertEqual(calls, [800, 800])
        joined = "\n".join(logs.output)
        self.assertIn("RuntimeError", joined)
        self.assertNotIn("still delivered", joined, "logs must never quote the user's text")

    async def test_internal_error_on_an_edited_command_goes_where_hermes_sends_commands(self):
        real = self.adapter._build_triggered_event
        failed = []

        async def flaky(msg, update, msg_type):
            if not failed:
                failed.append(msg_type)
                raise RuntimeError("simulated adapter change")
            return await real(msg, update, msg_type)

        self.mp.setattr(self.adapter, "_build_triggered_event", flaky)
        with self.assertLogs(LOGGER, "WARNING"):
            await self.app.process_update(upd(self.app.bot, 810, "/status", edited=True, command=True, mid=491))
            await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)
        self.assertEqual(self.delivered[0].message_type, T.MessageType.COMMAND)
        self.assertEqual(self.delivered[0].get_command(), "status")

    async def test_edit_handler_matches_only_the_owners_dm_text_edits(self):
        [handler] = edit_handlers(self.app)
        bot = self.app.bot
        self.assertTrue(handler.check_update(upd(bot, 1000, "edited", edited=True)))
        cases = {
            "fresh message": upd(bot, 1001, "fresh"),
            "other user in the owner's chat": upd(bot, 1002, "x", edited=True, user=99),
            "owner in another chat": upd(bot, 1003, "x", edited=True, chat=42),
            "edited photo caption": T.update(bot, 1004, kind="photo", edited=True, chat=88),
            "edited live location": T.update(bot, 1005, kind="location", edited=True, chat=88),
            "sent via an inline bot": upd(bot, 1006, "x", edited=True, via_bot=True),
        }
        for name, update in cases.items():
            with self.subTest(name):
                self.assertFalse(handler.check_update(update))
        # The plugin's handler must sit before Hermes' own text handler, or core takes edits first.
        self.assertLess(self.app.handlers[0].index(handler), self.app.handlers[0].index(core_text_handler(self.app)))

    async def test_owner_edit_is_dropped_when_the_adapter_refuses_it(self):
        for uid, name in ((1101, "_is_user_authorized_from_message"), (1102, "_should_process_message")):
            with self.subTest(name):
                original = getattr(self.adapter, name)
                setattr(self.adapter, name, lambda *a, **k: False)
                try:
                    await self.app.process_update(upd(self.app.bot, uid, "refused", edited=True))
                    await drain(self.adapter)
                    self.assertEqual(self.delivered, [])
                finally:
                    setattr(self.adapter, name, original)
        # Positive control: with both checks passing, the same edit path does deliver.
        await self.app.process_update(upd(self.app.bot, 1103, "accepted", edited=True))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)
        self.assertTrue(self.delivered[0].text.endswith(corrected("accepted")))

    async def test_fresh_messages_stay_on_the_core_path(self):
        seen = []
        real = self.adapter._ensure_forum_commands

        async def spy(message):
            seen.append(message)
            return await real(message)

        self.mp.setattr(self.adapter, "_ensure_forum_commands", spy)
        await self.app.process_update(upd(self.app.bot, 1200, "plain new message", mid=500))
        await drain(self.adapter)
        self.assertEqual(len(seen), 1, "Hermes' own text handler must take new messages")
        self.assertEqual(self.delivered[0].text, "plain new message")
        self.assertTrue(self.delivered[0].allow_gateway_control)

    async def test_forced_reload_leaves_no_untagged_gap(self):
        state = main_state(self.app)
        loaded = state.current
        serving = []
        real = manager.unload

        def unload(*args, **kwargs):
            result = real(*args, **kwargs)
            serving.append(state.current)
            return result

        self.mp.setattr(manager, "unload", unload)
        await self.reload()
        self.assertTrue(serving, "a forced reload unloads plugins first")
        self.assertIs(serving[0], loaded, "after Hermes' unload step the loaded code still serves")
        self.assertNotIn(None, serving)
        self.assertIsNot(state.current, loaded, "the new load takes over")

    async def test_inert_handlers_leave_updates_to_hermes(self):
        state = main_state(self.app)
        loaded = state.current
        try:
            state.current = None                    # nothing loaded (never happens after a first load)
            with self.assertNoLogs(LOGGER, "WARNING"):
                await self.app.process_update(upd(self.app.bot, 1300, "nothing loaded", edited=True, mid=510))
                await drain(self.adapter)
                await self.app.process_update(upd(self.app.bot, 1301, "fresh, nothing loaded", mid=511))
                await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["nothing loaded", "fresh, nothing loaded"])
            self.assertTrue(self.delivered[0].allow_gateway_control, "Hermes' own path, untouched")
        finally:
            state.current = loaded

    async def test_unload_between_filter_and_handler_is_harmless(self):
        state = main_state(self.app)
        loaded = state.current
        real = loaded.accepts_message

        def accept_then_unload(message):
            result = real(message)
            state.current = None        # an unload lands between PTB's filter check and the callback
            return result

        loaded.accepts_message = accept_then_unload
        try:
            with self.assertNoLogs(LOGGER, "WARNING"):   # the recorder just skips
                await self.app.process_update(upd(self.app.bot, 2300, "fresh while unloading", mid=610))
                await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["fresh while unloading"])
            state.current = loaded
            with self.assertLogs(LOGGER, "WARNING"):     # the edit goes to Hermes, untagged
                await self.app.process_update(upd(self.app.bot, 2301, "edit while unloading", edited=True, mid=611))
                await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["fresh while unloading", "edit while unloading"])
        finally:
            del loaded.accepts_message
            state.current = loaded

    async def test_forced_rediscovery_keeps_edits_tagged_with_no_rewire(self):
        state = main_state(self.app)
        loaded = state.current
        before = handler_count(self.app)
        rewires = []
        real = self.adapter.rewire_plugin_handlers

        def counted():
            rewires.append(1)
            return real()

        self.mp.setattr(self.adapter, "rewire_plugin_handlers", counted)
        self.subscribe(self.adapter)
        await self.reload()   # e.g. `hermes plugins enable <another plugin>`, or a tool's forced re-discovery
        self.assertEqual(rewires, [], "Hermes re-wires only plugins it did not have loaded")
        self.assertIsNotNone(state.current)
        self.assertIsNot(state.current, loaded, "the reloaded code must take over")
        await self.app.process_update(upd(self.app.bot, 1400, "after reload", edited=True, mid=520))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 1)
        self.assertTrue(self.delivered[0].text.startswith(MARK))
        self.assertEqual(handler_count(self.app), before)

    async def test_disable_acts_at_once_and_enable_reloads_without_restart(self):
        before = handler_count(self.app)
        try:
            write_selection([], [KEY])              # `hermes plugins disable`: config only, no reload
            await self.app.process_update(upd(self.app.bot, 1500, "while disabled", edited=True, mid=530))
            await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["while disabled"])
            self.assertTrue(self.delivered[0].allow_gateway_control, "Hermes' own path, untouched")
            write_selection([KEY], [])              # `hermes plugins enable`: config, then a reload nudge...
            await self.reload()                     # ...which the gateway answers with this verb
            await self.app.process_update(upd(self.app.bot, 1501, "enabled again", edited=True, mid=531))
            await drain(self.adapter)
            self.assertEqual(len(self.delivered), 2)
            self.assertTrue(self.delivered[1].text.startswith(MARK))
            self.assertEqual(handler_count(self.app), before)
        finally:
            await self.restore_enabled()

    async def test_reload_puts_new_code_behind_the_live_handlers(self):
        source = INSTALLED.read_text()
        updated = source.replace("[Telegram edit: the user", "[Telegram edit (updated code): the user")
        self.assertEqual(updated.count("(updated code)"), 1)
        before = handler_count(self.app)
        try:
            INSTALLED.write_text(updated)
            await self.reload()                     # what install.sh's disable/enable cycle makes it run
            await self.app.process_update(upd(self.app.bot, 1600, "new code", edited=True, mid=540))
            await drain(self.adapter)
            self.assertEqual(len(self.delivered), 1)
            self.assertTrue(self.delivered[0].text.startswith("[Telegram edit (updated code): "))
            self.assertEqual(handler_count(self.app), before)
        finally:
            INSTALLED.write_text(source)
            await self.reload()

    async def test_plugin_enabled_after_connect_is_wired_ahead_of_core(self):
        second = MP()
        try:
            write_selection([], [KEY])
            await self.reload()                     # the gateway drops it; what was loaded goes inert
            await self.app.process_update(upd(self.app.bot, 1699, "while off", edited=True, mid=549))
            await drain(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["while off"])
            async with T.connected(second, bot_id=222) as (adapter, app, delivered):
                self.assertEqual(edit_handlers(app), [], "nothing to wire while the plugin is off")
                self.subscribe(adapter)
                write_selection([KEY], [])
                await self.reload()                 # loaded as a newcomer: the runner re-wires its adapters
                [handler] = edit_handlers(app)
                self.assertLess(app.handlers[0].index(handler), app.handlers[0].index(core_text_handler(app)))
                await app.process_update(upd(app.bot, 1700, "late", edited=True, mid=550))
                await drain(adapter)
                self.assertEqual(len(delivered), 1)
                self.assertTrue(delivered[0].text.startswith(MARK))
        finally:
            second.restore()
            await self.restore_enabled()

    async def test_enabled_rule_matches_hermes(self):
        from hermes_cli.plugins_discovery import (_get_disabled_plugins, _get_enabled_plugins, gate_manifest,
                                                  scan_directory)
        pkg = load_installed_plugin("parity")
        [manifest] = [m for m in scan_directory(HOME / "plugins", "user") if m.name == KEY]
        variants = [
            "plugins:\n  enabled: [telegram-edit-replacement]\n",
            "plugins:\n  enabled: [telegram-edit-replacement]\n  disabled: [telegram-edit-replacement]\n",
            "plugins:\n  enabled: [other]\n",
            "plugins: {enabled: [telegram-edit-replacement]\n",          # not valid YAML: keeps the last state
            "plugins:\n  enabled: []\n",
            "plugins:\n  enabled: telegram-edit-replacement\n",
            "plugins:\n  disabled: [other]\n",
            "plugins: {}\n",
            "{}\n",
            "plugins:\n  enabled: [telegram-edit-replacement]\n  disabled: oops\n",
            "plugins: {enabled: [telegram-edit-replacement]\n",          # not valid YAML: keeps the last state
        ]
        gate = pkg.ConfigGate(CONFIG)
        outcomes = set()
        try:
            for text in variants:
                with self.subTest(text=text):
                    CONFIG.write_text(text)
                    bump(CONFIG)
                    hermes = gate_manifest(manifest, _get_disabled_plugins(), _get_enabled_plugins()).action
                    self.assertEqual(gate.enabled(), hermes != "placeholder", hermes)
                    outcomes.add(hermes != "placeholder")
            self.assertEqual(outcomes, {True, False})
            self.assertFalse(pkg.ConfigGate(HOME / "no-such-config.yaml").enabled())
        finally:
            CONFIG.write_text(ENABLED_CONFIG)
            bump(CONFIG)

    # -- Caption edits ---------------------------------------------------------------------------------

    def media(self):
        """Stub Hermes' media download (Telegram's getFile and the media cache): every download is cached
        under a new name, as Hermes' cache does. Returns the file ids downloaded, in order."""
        from telegram import Audio, Document, PhotoSize, Video, Voice
        import gateway.platforms.base as base
        import plugins.platforms.telegram.adapter as tg
        downloads = []

        async def get_file(source, *args, **kwargs):
            downloads.append(source.file_unique_id)

            async def download_as_bytearray(*a, **k):
                return bytearray(b"offline notes")
            return types.SimpleNamespace(file_path="offline.jpg", download_as_bytearray=download_as_bytearray)

        def path(ext):
            return f"/nonexistent/media-{len(downloads)}{ext or '.bin'}"

        async def cache_file(data, ext=None):
            return path(ext)

        async def cache_media(data, filename=None, mime_type=None, **kwargs):
            # Hermes' own result type, so readers such as the replied-media note find display_name.
            return base.CachedMedia(path=path(os.path.splitext(filename or "")[1]),
                                    media_type=mime_type or "application/octet-stream", kind="document",
                                    display_name=filename or "file.bin")

        for cls in (Audio, Document, PhotoSize, Video, Voice):
            self.mp.setattr(cls, "get_file", get_file)
        for name in ("cache_image_from_bytes_async", "cache_video_from_bytes_async", "cache_audio_from_bytes_async"):
            self.mp.setattr(tg, name, cache_file)
        self.mp.setattr(base, "cache_media_bytes_async", cache_media)
        return downloads

    async def test_caption_handlers_sit_ahead_of_hermes_media_handler(self):
        [edited] = caption_handlers(self.app, 0)
        self.assertLess(self.app.handlers[0].index(edited), self.app.handlers[0].index(core_media_handler(self.app)))
        self.assertEqual(len(caption_handlers(self.app, 98)), 1, "the media recorder")
        state = main_state(self.app)
        sys.modules[type(state.current).__module__].make_caption_factory(state)(self.app, self.adapter)
        self.assertEqual((len(caption_handlers(self.app, 0)), len(caption_handlers(self.app, 98))), (1, 1),
                         "wired once per Telegram client")

    async def test_a_caption_edit_of_a_delivered_photo_gets_a_notice_without_the_photo(self):
        downloads = self.media()
        await self.app.process_update(media_upd(self.app.bot, 6000, "a red car", mid=3000))
        await drain_media(self.adapter)
        [photo] = self.delivered
        self.assertEqual((photo.text, len(photo.media_urls)), ("a red car", 1))
        with self.assertNoLogs(LOGGER, "WARNING"):
            await self.app.process_update(media_upd(self.app.bot, 6001, "a blue car", mid=3000, edited=True))
            self.assertEqual(len(self.delivered), 2, "dispatched at once, not after a batch delay")
            await drain_media(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        notice = self.delivered[1]
        self.assertEqual(notice.text, caption_notice(3000, "a red car", "a blue car"))
        self.assertEqual((notice.media_urls, notice.allow_gateway_control), ([], False))
        self.assertEqual(downloads, ["file-3000"], "the photo is never downloaded again")
        bold = [{"type": "bold", "offset": 0, "length": 6}]
        await self.app.process_update(media_upd(self.app.bot, 6002, "a blue car", mid=3000, entities=bold,
                                                edited=True))
        await drain_media(self.adapter)
        self.assertEqual(len(self.delivered), 2, "formatting only, after a real edit: nothing")

    async def test_a_caption_edit_while_the_photo_waits_in_its_burst_travels_with_it(self):
        downloads = self.media()
        await self.app.process_update(media_upd(self.app.bot, 6010, "a red car", mid=3010))
        await self.app.process_update(media_upd(self.app.bot, 6011, "a blue car", mid=3010, edited=True))
        await drain_media(self.adapter)
        [photo] = self.delivered
        self.assertEqual(photo.text, "a red car\n" + caption_notice(3010, "a red car", "a blue car"))
        self.assertEqual((len(photo.media_urls), downloads), (1, ["file-3010"]))
        self.assertTrue(photo.allow_gateway_control, "the user's own message keeps its gateway control")

    async def test_a_caption_edit_of_a_later_photo_in_a_waiting_burst_travels_with_it(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6020, "first", mid=3020))
        await self.app.process_update(media_upd(self.app.bot, 6021, "second", mid=3021))
        await self.app.process_update(media_upd(self.app.bot, 6022, "second, fixed", mid=3021, edited=True))
        await drain_media(self.adapter)
        [burst] = self.delivered
        self.assertEqual(burst.text, "first\n\nsecond\n" + caption_notice(3021, "second", "second, fixed"))
        self.assertEqual(len(burst.media_urls), 2)

    async def test_a_caption_added_to_an_album_item_travels_with_the_album(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6030, "trip photos", mid=3030, album="alb1"))
        await self.app.process_update(media_upd(self.app.bot, 6031, mid=3031, album="alb1"))
        await self.app.process_update(media_upd(self.app.bot, 6032, "the beach", mid=3031, album="alb1",
                                                edited=True))
        await drain_media(self.adapter)
        [album] = self.delivered
        self.assertEqual(album.text, "trip photos\n" + caption_notice(3031, "", "the beach"))
        self.assertEqual(len(album.media_urls), 2)

    async def test_a_caption_edit_while_the_photo_waits_for_the_next_turn_travels_with_it(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 6040, "a red car", mid=3040))
            await drain_media(self.adapter)
            slot = self.adapter._pending_messages[key]
            self.assertEqual(slot.text, "a red car")
            await self.app.process_update(media_upd(self.app.bot, 6041, "a blue car", mid=3040, edited=True))
            await self.app.process_update(media_upd(self.app.bot, 6042, "a green car", mid=3040, edited=True))
            await drain_media(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], slot)
            self.assertEqual(slot.text, "a red car\n" + caption_notice(3040, "a red car", "a blue car") + "\n"
                             + caption_notice(3040, "a blue car", "a green car"))
            bold = [{"type": "bold", "offset": 0, "length": 7}]
            await self.app.process_update(media_upd(self.app.bot, 6043, "a green car", mid=3040, entities=bold,
                                                    edited=True))
            await drain_media(self.adapter)
            self.assertTrue(slot.text.endswith(caption_notice(3040, "a blue car", "a green car")),
                            "formatting only, after a real edit of a waiting photo: nothing")
            self.assertEqual((self.delivered, runner.calls), ([], 1), "nothing else went through the busy lane")

    async def test_a_caption_edit_of_a_photo_merged_into_a_queued_question_finds_it_there(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 6050, "what is this?", mid=3050))
            await drain_media(self.adapter)
            await self.app.process_update(media_upd(self.app.bot, 6051, "a red car", mid=3051))
            await drain_media(self.adapter)
            slot = self.adapter._pending_messages[key]
            self.assertEqual((slot.text, len(slot.media_urls)), ("what is this?\n\na red car", 1))
            await self.app.process_update(media_upd(self.app.bot, 6052, "a blue car", mid=3051, edited=True))
            await drain_media(self.adapter)
            self.assertEqual(slot.text, "what is this?\n\na red car\n" + caption_notice(3051, "a red car",
                                                                                         "a blue car"))
            self.assertEqual(self.delivered, [])

    async def test_a_caption_edit_of_a_file_in_the_runners_fifo_travels_with_it(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            first = self.queue_photo(runner, key, 3060)     # a photo the plugin never recorded: its path is unknown
            await self.app.process_update(media_upd(self.app.bot, 6061, "the report", mid=3061, kind="document"))
            await drain_media(self.adapter)
            [queued] = runner._overflow_queue(key)
            await self.app.process_update(media_upd(self.app.bot, 6062, "the Q3 report", mid=3061,
                                                    kind="document", edited=True))
            await drain_media(self.adapter)
            self.assertEqual(queued.text, "the report\n" + caption_notice(3061, "the report", "the Q3 report",
                                                                          kind="file"))
            self.assertEqual((self.adapter._pending_messages[key], first.text), (first, ""))
            self.assertEqual(self.delivered, [])

    async def test_formatting_only_caption_edit_of_a_text_file_changes_nothing(self):
        downloads = self.media()
        await self.app.process_update(media_upd(self.app.bot, 6070, "summarise this", mid=3070,
                                                kind="text document"))
        await drain_media(self.adapter)
        [doc] = self.delivered
        self.assertEqual(doc.text, "[Content of notes.txt]:\noffline notes\n\nsummarise this")
        bold = [{"type": "bold", "offset": 0, "length": 9}]
        await self.app.process_update(media_upd(self.app.bot, 6071, "summarise this", mid=3070,
                                                kind="text document", entities=bold, edited=True))
        await drain_media(self.adapter)
        self.assertEqual((self.delivered, downloads), ([doc], ["file-3070"]))
        await self.app.process_update(media_upd(self.app.bot, 6072, "summarise it", mid=3070,
                                                kind="text document", edited=True))
        await drain_media(self.adapter)
        self.assertEqual(self.delivered[1].text, caption_notice(3070, "summarise this", "summarise it", kind="file"))

    async def test_removing_a_caption_says_so(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6080, "a clip", mid=3080, kind="video"))
        await drain_media(self.adapter)
        await self.app.process_update(media_upd(self.app.bot, 6081, mid=3080, kind="video", edited=True))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["a clip", caption_notice(3080, "a clip", "",
                                                                                     kind="video")])

    async def test_a_caption_edited_into_a_command_is_never_run(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6090, "look", mid=3090))
        await drain_media(self.adapter)
        await self.app.process_update(media_upd(self.app.bot, 6091, "/status", mid=3090, edited=True))
        await drain_media(self.adapter)
        notice = self.delivered[1]
        self.assertEqual(notice.text, caption_notice(3090, "look", "/status", command=True))
        self.assertIsNone(notice.get_command())
        self.assertEqual(len(self.delivered), 2)

    async def test_a_replaced_photo_is_left_as_hermes_leaves_it(self):
        downloads = self.media()
        await self.app.process_update(media_upd(self.app.bot, 6100, "a red car", mid=3100, file="car-a"))
        await drain_media(self.adapter)
        with self.assertNoLogs(LOGGER, "WARNING"):
            await self.app.process_update(media_upd(self.app.bot, 6101, "a blue car", mid=3100, file="car-b",
                                                    edited=True))
            await drain_media(self.adapter)
        self.assertEqual((len(self.delivered), downloads), (1, ["car-a"]))
        await self.app.process_update(media_upd(self.app.bot, 6102, "a blue car", mid=3100, file="car-a",
                                                edited=True))
        await drain_media(self.adapter)
        self.assertEqual(self.delivered[1].text, caption_notice(3100, "a red car", "a blue car"))

    async def test_a_caption_edit_of_an_unknown_message_says_the_earlier_caption_is_not_available(self):
        downloads = self.media()
        await self.app.process_update(media_upd(self.app.bot, 6110, "call mum", mid=3110, kind="voice",
                                                edited=True))
        await drain_media(self.adapter)
        [notice] = self.delivered
        self.assertEqual(notice.text, caption_notice(3110, None, "call mum", kind="voice message"))
        self.assertEqual(downloads, [])

    async def test_a_photo_whose_caption_reads_as_a_command_is_never_given_a_notice(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6120, "/status", mid=3120))
        [burst] = self.adapter._pending_photo_batches.values()
        await self.app.process_update(media_upd(self.app.bot, 6121, "status please", mid=3120, edited=True))
        self.assertEqual(burst.text, "/status", "a notice appended there would become the command's arguments")
        self.assertEqual([e.text for e in self.delivered], [caption_notice(3120, "/status", "status please")])
        await drain_media(self.adapter)

    async def test_inert_or_older_code_leaves_edited_media_to_hermes(self):
        self.media()
        state = main_state(self.app)
        loaded = state.current
        try:
            for current in (None, types.SimpleNamespace(accepts_message=lambda message: True)):
                with self.subTest(current=current):
                    state.current = current
                    with self.assertNoLogs(LOGGER, "WARNING"):
                        await self.app.process_update(media_upd(self.app.bot, 6130 + len(self.delivered), "x",
                                                                mid=3130 + len(self.delivered)))
                        await drain_media(self.adapter)
                        await self.app.process_update(media_upd(self.app.bot, 6135 + len(self.delivered), "y",
                                                                mid=3129 + len(self.delivered), edited=True))
                        await drain_media(self.adapter)
            self.assertEqual([e.text for e in self.delivered], ["x", "x"], "fresh media is Hermes' own; "
                             "the edits are dropped by Hermes, as without the plugin")
        finally:
            state.current = loaded

    async def test_an_error_before_anything_changed_delivers_nothing(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6140, "a red car", mid=3140))
        await drain_media(self.adapter)

        def broken(*args, **kwargs):
            raise RuntimeError("offline")

        self.mp.setattr(self.adapter, "_pop_text_batch", broken)    # the pending batch goes first: it fails
        with self.assertLogs(LOGGER, "WARNING") as logs:
            await self.app.process_update(media_upd(self.app.bot, 6141, "a blue car", mid=3140, edited=True))
            await drain_media(self.adapter)
        self.assertIn("could not handle a caption edit; nothing delivered", logs.output[0])
        self.assertNotIn("blue", "".join(logs.output))
        self.assertEqual(len(self.delivered), 1)

    async def test_an_error_after_the_notice_was_queued_never_delivers_it_twice(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6150, "a red car", mid=3150))
        await drain_media(self.adapter)
        cache = main_state(self.app).current.cache
        real = cache.put
        calls = []

        def put(chat_id, message_id, seen):
            calls.append(seen)
            if len(calls) > 0 and getattr(seen, "wording", None) == "a blue car":
                raise RuntimeError("offline")
            return real(chat_id, message_id, seen)

        self.mp.setattr(cache, "put", put)
        with self.assertLogs(LOGGER, "WARNING") as logs:
            await self.app.process_update(media_upd(self.app.bot, 6151, "a blue car", mid=3150, edited=True))
            await drain_media(self.adapter)
        self.assertIn("error after queueing a caption edit", logs.output[0])
        self.assertEqual([e.text for e in self.delivered], ["a red car", caption_notice(3150, "a red car",
                                                                                         "a blue car")])

    async def test_caption_handlers_are_wired_into_a_client_connected_while_the_plugin_was_off(self):
        second = MP()
        try:
            write_selection([], [KEY])
            await self.reload()
            async with T.connected(second, bot_id=223) as (adapter, app, delivered):
                self.assertEqual(caption_handlers(app, 0), [], "nothing to wire while the plugin is off")
                self.subscribe(adapter)
                write_selection([KEY], [])
                with self.assertLogs(LOGGER, "INFO") as logs:
                    await self.reload()             # loaded as a newcomer: the runner re-wires its adapters
                self.assertNotIn("restart", "".join(logs.output))
                [handler] = caption_handlers(app, 0)
                self.assertLess(app.handlers[0].index(handler), app.handlers[0].index(core_media_handler(app)))
                self.assertEqual(len(caption_handlers(app, 98)), 1)
        finally:
            second.restore()
            await self.restore_enabled()

    async def test_a_caption_added_while_a_photo_without_one_waits_becomes_its_text(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6160, mid=3160))
        await self.app.process_update(media_upd(self.app.bot, 6161, "my cat", mid=3160, edited=True))
        await drain_media(self.adapter)
        [photo] = self.delivered
        self.assertEqual(photo.text, caption_notice(3160, "", "my cat"))

    async def test_a_second_caption_edit_joins_the_first_notice_while_it_waits(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6170, "a red car", mid=3170))
        await drain_media(self.adapter)
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 6171, "a blue car", mid=3170, edited=True))
            await drain_media(self.adapter)
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(media_upd(self.app.bot, 6172, "a green car", mid=3170, edited=True))
            await drain_media(self.adapter)
            self.assertEqual(slot.text, caption_notice(3170, "a red car", "a blue car") + "\n"
                             + caption_notice(3170, "a blue car", "a green car"))
            self.assertEqual(runner._overflow_queue(key) or [], [])
        self.assertEqual(len(self.delivered), 1)

    async def test_a_caption_edit_after_its_burst_merged_into_the_slot_follows_the_photo(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 6180, "what is this?", mid=3180))
            await drain_media(self.adapter)
            await self.app.process_update(media_upd(self.app.bot, 6181, "a red car", mid=3181))
            await self.app.process_update(media_upd(self.app.bot, 6182, "a blue car", mid=3181, edited=True))
            await drain_media(self.adapter)             # the burst, notice included, merges into the slot
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(media_upd(self.app.bot, 6183, "a green car", mid=3181, edited=True))
            await drain_media(self.adapter)
            self.assertEqual(slot.text, "what is this?\n\na red car\n" + caption_notice(3181, "a red car", "a blue car")
                             + "\n" + caption_notice(3181, "a blue car", "a green car"))
            self.assertEqual(self.delivered, [])

    async def test_a_pending_text_goes_before_the_caption_notice(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6190, "a red car", mid=3190))
        await drain_media(self.adapter)
        await self.app.process_update(upd(self.app.bot, 6191, "and another thing", mid=3191))
        await self.app.process_update(media_upd(self.app.bot, 6192, "a blue car", mid=3190, edited=True))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered],
                         ["a red car", "and another thing", caption_notice(3190, "a red car", "a blue car")])

    async def test_a_cancel_while_the_pending_text_goes_first_holds_both_in_order(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6200, "a red car", mid=3200))
        await drain_media(self.adapter)
        await self.app.process_update(upd(self.app.bot, 6201, "pending message", mid=3201))

        async def cancelled(event):
            raise asyncio.CancelledError()

        self.mp.setattr(self.adapter, "_dispatch_text_batch", cancelled)
        update = media_upd(self.app.bot, 6202, "a blue car", mid=3200, edited=True)
        with self.assertRaises(asyncio.CancelledError):
            await main_state(self.app).current.handle_caption_edit(update, None, self.adapter)
        held = list(self.adapter._held_inbound_events)
        self.assertEqual([e.text for e in held], ["pending message", caption_notice(3200, "a red car", "a blue car")])

    async def test_a_disabled_plugin_delivers_no_caption_notice(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6210, "a red car", mid=3210))
        await drain_media(self.adapter)
        try:
            write_selection([], [KEY])      # what `hermes plugins disable` commits; read on the next update
            await self.app.process_update(media_upd(self.app.bot, 6211, "a blue car", mid=3210, edited=True))
            await drain_media(self.adapter)
            self.assertEqual(len(self.delivered), 1, "Hermes drops edited media, as without the plugin")
        finally:
            CONFIG.write_text(ENABLED_CONFIG)
            bump(CONFIG)

    async def test_two_waiting_files_the_plugin_never_saw_are_never_guessed_between(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with self.busy(runner) as key:
            first = self.queue_photo(runner, key, 3220)     # unknown to the plugin
            await self.app.process_update(media_upd(self.app.bot, 6220, "first", mid=3221))
            main_state(self.app).wording.clear()            # as if the plugin loaded after this photo came
            await self.app.process_update(media_upd(self.app.bot, 6221, "second", mid=3222))
            [burst] = self.adapter._pending_photo_batches.values()
            await self.app.process_update(media_upd(self.app.bot, 6222, "second, fixed", mid=3222, edited=True))
            self.assertEqual((first.text, burst.text), ("", "first\n\nsecond"), "no waiting event is guessed")
            self.assertEqual(self.adapter._pending_messages[key], first)
            [notice] = runner._overflow_queue(key)
            self.assertEqual(notice.text, caption_notice(3222, "second", "second, fixed"))
            await drain_media(self.adapter)

    async def test_queue_mode_a_second_caption_edit_joins_the_first_notice_in_the_debounce_buffer(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 6230, "a red car", mid=3230))
        await drain_media(self.adapter)
        with self.busy(mode="queue") as key:
            await self.app.process_update(media_upd(self.app.bot, 6231, "a blue car", mid=3230, edited=True))
            waiting = self.adapter._text_debounce_store()[key].event
            await self.app.process_update(media_upd(self.app.bot, 6232, "a green car", mid=3230, edited=True))
            self.assertEqual(waiting.text, caption_notice(3230, "a red car", "a blue car") + "\n"
                             + caption_notice(3230, "a blue car", "a green car"))
        self.assertEqual(len(self.delivered), 1)

    async def test_a_failing_caption_filter_leaves_the_update_to_hermes(self):
        self.media()
        state = main_state(self.app)
        loaded = state.current

        def broken(message):
            raise RuntimeError("offline")

        try:
            state.current = types.SimpleNamespace(handle_caption_edit=loaded.handle_caption_edit,
                                                  record_media=loaded.record_media, accepts_message=broken)
            with self.assertLogs(LOGGER, "WARNING") as logs:
                await self.app.process_update(media_upd(self.app.bot, 6240, "a red car", mid=3240))
                await drain_media(self.adapter)
            self.assertIn("caption filter failed; Hermes handles the update", logs.output[0])
            self.assertEqual([e.text for e in self.delivered], ["a red car"])
        finally:
            state.current = loaded

    async def test_a_failing_media_recorder_only_logs(self):
        self.media()
        gen = main_state(self.app).current

        def broken():
            raise RuntimeError("offline")

        self.mp.setattr(gen, "_known_paths", broken)
        with self.assertLogs(LOGGER, "WARNING") as logs:
            await self.app.process_update(media_upd(self.app.bot, 6250, "a red car", mid=3250))
            await drain_media(self.adapter)
        self.assertIn("could not record a media message", logs.output[0])
        self.assertEqual([e.text for e in self.delivered], ["a red car"])

    async def test_media_waiting_in_another_chat_or_topic_is_never_taken(self):
        from gateway.session_identity import replace_source
        self.media()
        for n, (where, chat, topic) in enumerate((("another chat", 99, None), ("another topic", 88, "5"))):
            with self.subTest(where=where):
                other = self.adapter._build_message_event(
                    media_upd(self.app.bot, 0, "theirs", mid=3260, user=chat, chat=chat).message, T.MessageType.PHOTO)
                if topic:
                    other.source = replace_source(other.source, thread_id=topic)
                other.media_urls, other.media_types = [f"/nonexistent/other-{n}.jpg"], ["image/jpeg"]
                key = self.adapter._event_session_key(other)
                self.adapter._pending_messages[key] = other
                try:
                    await self.app.process_update(media_upd(self.app.bot, 6260 + 2 * n, "the report", mid=3261 + n,
                                                            kind="document"))
                    await drain_media(self.adapter)
                    await self.app.process_update(media_upd(self.app.bot, 6261 + 2 * n, "the Q3 report",
                                                            mid=3261 + n, kind="document", edited=True))
                    await drain_media(self.adapter)
                    self.assertEqual(other.text, "theirs", "a waiting message elsewhere is never touched")
                    self.assertEqual([e.text for e in self.delivered[-2:]],
                                     ["the report", caption_notice(3261 + n, "the report", "the Q3 report",
                                                                   kind="file")])
                finally:
                    self.adapter._pending_messages.pop(key, None)

    # -- What Hermes derives from a waiting event (its speech-to-text cache) --------------------------------

    async def test_a_caption_edit_of_a_waiting_voice_note_reaches_its_turn_after_hermes_transcribed_it(self):
        """Interrupt mode: Hermes queues the voice note and transcribes it at once for the interrupt, caching the
        text on the event (run_inbound.py:1996-2010); the drain then reads that cache, not .text."""
        self.media()
        runner = BusyRunner(self.adapter)
        runner.interrupting = True
        clips, echoes = with_stt(runner)
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 8000, "call mum", mid=5000, kind="voice"))
            await drain_media(self.adapter)
            queued = self.adapter._pending_messages[key]
            self.assertEqual((queued.text, len(clips), runner.interrupts), ("call mum", 1, ['"spoken 1"\n\ncall mum']))
            self.assertTrue(hasattr(queued, "_gateway_pending_stt_text"), "Hermes cached the transcript")
            await self.app.process_update(media_upd(self.app.bot, 8001, "call dad", mid=5000, kind="voice",
                                                    edited=True))
            await drain_media(self.adapter)
            self.assertEqual(self.delivered, [])
            event, pending = await runner.next_turn(key, queued.source)
            self.assertIs(event, queued)
            self.assertEqual(pending, '"spoken 2"\n\ncall mum\n' + caption_notice(5000, "call mum", "call dad",
                                                                                   kind="voice message"))
            self.assertEqual(echoes, ["spoken 1"], "transcribed again, never echoed twice")

    async def test_a_text_edit_appended_to_a_waiting_voice_note_hermes_transcribed_reaches_its_turn(self):
        """The text path's sibling: the runner merged a voice note into a queued text (run_inbound.py:714, an agent
        still being set up), then the interrupt monitor transcribed the slot (run_turn.py:3296-3315)."""
        from gateway.run import GatewayRunner
        self.media()
        runner = BusyRunner(self.adapter, merge=True)
        clips, echoes = with_stt(runner)
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 8010, "summarise the call", mid=5010))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            await self.app.process_update(media_upd(self.app.bot, 8011, mid=5011, kind="voice"))
            await drain_media(self.adapter)
            self.assertIs(self.adapter._pending_messages[key], slot)
            self.assertEqual((slot.message_type, len(slot.media_urls)), (T.MessageType.VOICE, 1))
            agent = types.SimpleNamespace(interrupt=runner.interrupts.append)
            await GatewayRunner._run_agent_fire_pending_interrupt(
                runner, self.adapter, agent, slot.source, key, asyncio.Event(), [None],
                log_context="Voice-interrupt", log=lambda: None)
            self.assertTrue(hasattr(slot, "_gateway_pending_stt_text"), "Hermes cached the transcript")
            await self.app.process_update(upd(self.app.bot, 8012, "summarise the call briefly", edited=True,
                                              mid=5010))
            await drain(self.adapter)
            self.assertEqual(self.delivered, [])
            event, pending = await runner.next_turn(key, slot.source)
            self.assertIs(event, slot)
            self.assertTrue(pending.startswith('"spoken 2"\n\nsummarise the call\n' + MARK), pending)
            self.assertTrue(pending.endswith(corrected("summarise the call briefly")), pending)
            self.assertEqual(echoes, ["spoken 1"])

    def test_the_plugin_drops_what_hermes_merge_drops_from_a_waiting_event(self):
        """Parity with Hermes' own change to a waiting event (merge_pending_message_event, base.py:1834-1838): the
        derived speech-to-text cache goes, the echo ledger stays."""
        from gateway.platforms.base import merge_pending_message_event
        pkg = sys.modules[type(main_state(self.app).current).__module__]
        waiting = self.adapter._build_message_event(upd(self.app.bot, 0, "x", mid=5090).message, T.MessageType.VOICE)
        waiting.media_urls, waiting.media_types = ["/nonexistent/v.ogg"], ["audio/ogg"]
        for name, value in (("_gateway_pending_stt_text", "cached"), ("_gateway_pending_stt_transcripts", ["t"]),
                            ("_gateway_pending_stt_echoed", 1)):
            setattr(waiting, name, value)
        ours, hermes = copy.copy(waiting), copy.copy(waiting)
        incoming = self.adapter._build_message_event(upd(self.app.bot, 0, "y", mid=5091).message, T.MessageType.TEXT)
        merge_pending_message_event({"k": hermes}, "k", incoming, merge_text=True)
        pkg._set_waiting_text(ours, "x\nnotice")

        def kept(event):
            return {name: getattr(event, name) for name in vars(event) if name.startswith("_gateway_pending_")}

        self.assertEqual(kept(ours), kept(hermes))
        self.assertEqual(kept(ours), {"_gateway_pending_stt_echoed": 1})
        self.assertEqual(ours.text, "x\nnotice")

    # -- A carrier whose text Hermes reads as a command ---------------------------------------------------

    async def test_a_caption_notice_never_joins_a_burst_that_starts_with_another_photos_command(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8020, None, mid=5020))
        await self.app.process_update(media_upd(self.app.bot, 8021, "/background look at these", mid=5021))
        [burst] = self.adapter._pending_photo_batches.values()
        await self.app.process_update(media_upd(self.app.bot, 8022, "the left one", mid=5020, edited=True))
        self.assertEqual(burst.text, "/background look at these", "a notice there becomes the command's arguments")
        self.assertEqual([e.text for e in self.delivered], [caption_notice(5020, "", "the left one")])
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered][1:], ["/background look at these"])

    async def test_a_text_edit_never_joins_a_batch_that_starts_with_a_long_command(self):
        long_command = "/notacommand" + " word" * 800
        self.assertGreaterEqual(len(long_command), self.adapter._SPLIT_THRESHOLD)
        await self.app.process_update(upd(self.app.bot, 8030, long_command, command=True, mid=5030))
        await self.app.process_update(upd(self.app.bot, 8031, "and this", mid=5031))
        [batch] = self.adapter._pending_text_batches.values()
        self.assertEqual(batch.text, long_command + "\nand this", "Hermes batched the text after the command")
        await self.app.process_update(upd(self.app.bot, 8032, "and these", edited=True, mid=5031))
        await drain(self.adapter)
        self.assertEqual(len(self.delivered), 2)
        self.assertEqual(self.delivered[0].text, long_command + "\nand this")
        self.assertTrue(self.delivered[1].text.startswith(MARK))
        self.assertTrue(self.delivered[1].text.endswith(corrected("and these")))

    # -- The recorder's unknown-path rule only where Hermes merged the message -----------------------------

    async def test_a_steered_voice_note_never_takes_a_queued_replys_media(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with_stt(runner)
        runner.steering = True
        with self.busy(runner) as key:
            await self.app.process_update(reply_upd(self.app.bot, 8040, "what about this one?", mid=5040))
            await drain(self.adapter)
            slot = self.adapter._pending_messages[key]
            self.assertEqual((slot.text.split("\n")[0], len(slot.media_urls)), ("what about this one?", 1),
                             "the reply waits with its replied-to photo")
            reply_text = slot.text
            await self.app.process_update(media_upd(self.app.bot, 8041, "call mum", mid=5041, kind="voice"))
            await drain_media(self.adapter)
            self.assertEqual(len(runner.steered), 1, "the voice note went into the running turn")
            await self.app.process_update(media_upd(self.app.bot, 8042, "call dad", mid=5041, kind="voice",
                                                    edited=True))
            self.assertEqual(slot.text, reply_text, "the queued reply is another message")
            self.assertEqual([e.text for e in runner.steered[1:]],
                             [caption_notice(5041, "call mum", "call dad", kind="voice message")])
            await drain_media(self.adapter)

    async def test_a_document_never_takes_the_path_of_a_photo_with_a_command_caption(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8050, "/status", mid=5050))
        [burst] = self.adapter._pending_photo_batches.values()
        await self.app.process_update(media_upd(self.app.bot, 8051, "the report", mid=5051, kind="document"))
        self.assertEqual([e.text for e in self.delivered], ["the report"], "straight to a turn")
        await self.app.process_update(media_upd(self.app.bot, 8052, "the Q3 report", mid=5051, kind="document",
                                                edited=True))
        self.assertEqual(burst.text, "/status")
        self.assertEqual([e.text for e in self.delivered][1:],
                         [caption_notice(5051, "the report", "the Q3 report", kind="file")])
        await drain_media(self.adapter)

    async def test_an_album_item_after_one_with_a_command_caption_travels_with_the_album(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8055, "trip photos", mid=5055, album="alb2"))
        await self.app.process_update(media_upd(self.app.bot, 8056, "/status", mid=5056, album="alb2"))
        await self.app.process_update(media_upd(self.app.bot, 8057, mid=5057, album="alb2"))
        await self.app.process_update(media_upd(self.app.bot, 8058, "the beach", mid=5057, album="alb2",
                                                edited=True))
        await drain_media(self.adapter)
        [album] = self.delivered
        self.assertEqual(album.text, "trip photos\n\n/status\n" + caption_notice(5057, "", "the beach"),
                         "the command-captioned item's path is known, so the last one is found")
        self.assertEqual(len(album.media_urls), 3)

    async def test_an_image_document_merged_into_a_waiting_burst_is_found_there(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8065, "first", mid=5065))
        await self.app.process_update(media_upd(self.app.bot, 8066, "a screenshot", mid=5066, kind="image document"))
        [burst] = self.adapter._pending_photo_batches.values()
        self.assertEqual(len(burst.media_urls), 2, "Hermes batched the image document with the photo")
        await self.app.process_update(media_upd(self.app.bot, 8067, "the screenshot", mid=5066,
                                                kind="image document", edited=True))
        await drain_media(self.adapter)
        [delivered] = self.delivered
        self.assertEqual(delivered.text, "first\n\na screenshot\n" + caption_notice(5066, "a screenshot",
                                                                                     "the screenshot", kind="file"))

    # -- A caption's command read as Hermes reads it for media --------------------------------------------

    async def test_a_photo_captioned_with_an_admin_phrase_is_conversation_and_gets_its_notice_in_place(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8060, "restart the gateway", mid=5060))
        await self.app.process_update(media_upd(self.app.bot, 8061, "restart the gateway in this picture",
                                                mid=5060, edited=True))
        await drain_media(self.adapter)
        [photo] = self.delivered
        self.assertEqual(photo.text, "restart the gateway\n" + caption_notice(
            5060, "restart the gateway", "restart the gateway in this picture"))

    async def test_a_caption_corrected_to_an_admin_phrase_is_not_called_a_command(self):
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8062, "look", mid=5062))
        await drain_media(self.adapter)
        await self.app.process_update(media_upd(self.app.bot, 8063, "restart hermes", mid=5062, edited=True))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered][1:], [caption_notice(5062, "look", "restart hermes")])

    # -- Hot wiring, teardown and caption edge cases ----------------------------------------------------------

    async def test_captions_are_wired_hot_onto_a_client_where_the_text_handlers_already_are(self):
        """An upgrade: an earlier version wired wire_telegram on this client; only wire_captions is new."""
        second = MP()
        try:
            async with T.connected(second, bot_id=224) as (adapter, app, delivered):
                state = main_state(app)
                for group in (0, 98):
                    for handler in caption_handlers(app, group):
                        app.handlers[group].remove(handler)
                adapter._plugin_handlers_wired = {key for key in adapter._plugin_handlers_wired
                                                  if "wire_captions" not in str(key[1])}
                _caption = getattr(state, "caption_wired", {})
                _caption.pop(app, None)
                self.assertEqual((len(edit_handlers(app)), caption_handlers(app, 0)), (1, []),
                                 "as the previous version left it")
                self.subscribe(adapter)
                write_selection([], [KEY])
                await self.reload()
                write_selection([KEY], [])
                with self.assertLogs(LOGGER, "INFO") as logs:
                    await self.reload()             # install.sh's enable: a newcomer, the runner re-wires
                self.assertNotIn("restart", "".join(logs.output))
                self.assertIn("caption handlers wired", "".join(logs.output))
                [text] = edit_handlers(app)
                [caption] = caption_handlers(app, 0)
                order = app.handlers[0]
                self.assertLess(order.index(text), order.index(caption))
                self.assertLess(order.index(caption), order.index(core_media_handler(app)))
                self.assertEqual(len(caption_handlers(app, 98)), 1)
        finally:
            second.restore()
            await self.restore_enabled()

    async def test_a_formatting_only_edit_of_a_long_caption_delivers_nothing(self):
        self.media()
        caption = ("word " * 300).strip()
        self.assertGreater(len(caption), 1400)
        await self.app.process_update(media_upd(self.app.bot, 8080, caption, mid=5080))
        await drain_media(self.adapter)
        bold = [{"type": "bold", "offset": 1490, "length": 4}]
        await self.app.process_update(media_upd(self.app.bot, 8081, caption, mid=5080, entities=bold, edited=True))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered], [caption])

    async def test_a_caption_edit_during_teardown_is_held_and_goes_after_the_photo(self):
        self.media()
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)
        await self.app.process_update(media_upd(self.app.bot, 8085, "a red car", mid=5085))
        await self.app.process_update(media_upd(self.app.bot, 8086, "a blue car", mid=5085, edited=True))
        self.assertEqual(self.delivered, [])
        await self.teardown_and_reconnect()
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered],
                         ["a red car", caption_notice(5085, "a red car", "a blue car")])

    async def test_a_caption_edit_of_a_steered_voice_note_goes_out_at_once_on_its_own(self):
        self.media()
        runner = BusyRunner(self.adapter)
        with_stt(runner)
        runner.steering = True
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 8090, "call mum", mid=5095, kind="voice"))
            await drain_media(self.adapter)
            self.assertEqual(len(runner.steered), 1)
            self.assertNotIn(key, self.adapter._pending_messages, "steered, queued nowhere")
            await self.app.process_update(media_upd(self.app.bot, 8091, "call dad", mid=5095, kind="voice",
                                                    edited=True))
            self.assertEqual([e.text for e in runner.steered[1:]],
                             [caption_notice(5095, "call mum", "call dad", kind="voice message")])
            await drain_media(self.adapter)

    # -- Guards that need a dedicated case -------------------------------------------------------------------

    async def test_a_command_captioned_photo_behind_another_caption_gets_its_notice_on_its_own(self):
        """A photo whose caption Hermes reads as a command gets no carrier, also where Hermes merged
        that caption behind another photo's, so the burst's own text reads as conversation."""
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8120, "look", mid=5120))
        await self.app.process_update(media_upd(self.app.bot, 8121, "/status", mid=5121))
        [burst] = self.adapter._pending_photo_batches.values()
        self.assertEqual(burst.text, "look\n\n/status", "Hermes merged the command caption into the burst's text")
        await self.app.process_update(media_upd(self.app.bot, 8122, "the sunset", mid=5121, edited=True))
        self.assertEqual(burst.text, "look\n\n/status", "no notice joins the burst")
        self.assertEqual([e.text for e in self.delivered], [caption_notice(5121, "/status", "the sunset")])
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered][1:], ["look\n\n/status"])

    async def test_an_admin_phrase_edited_into_another_while_it_waits_runs_neither(self):
        """A waiting text that holds only the edited message's own words keeps the notice even when those
        words are an admin phrase. Appended, the notice voids the phrase; sent on its own, it would leave the
        phrase waiting, and Hermes would run it as /restart when the batch goes out."""
        await self.app.process_update(upd(self.app.bot, 8130, "restart the gateway", mid=5130))
        [batch] = self.adapter._pending_text_batches.values()
        self.assertEqual(batch.text, "restart the gateway")
        await self.app.process_update(upd(self.app.bot, 8131, "restart hermes", edited=True, mid=5130))
        await drain(self.adapter)
        texts = [e.text for e in self.delivered]
        self.assertEqual(len(texts), 1, texts)
        self.assertTrue(texts[0].startswith("restart the gateway\n"), texts[0][:80])
        self.assertIn(NOT_RUN, texts[0])
        self.assertTrue(texts[0].endswith(corrected("restart hermes")))

    async def test_a_photo_held_in_teardown_never_takes_a_waiting_bursts_unknown_path(self):
        """A photo Hermes holds during teardown was merged nowhere, so the burst its key names is not where it
        waits, even when that burst holds one path no record knows (as media sent before the caption handlers
        were wired leaves). Its caption edit is held and goes after it."""
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8140, "a red car", mid=5140))
        [burst] = self.adapter._pending_photo_batches.values()
        burst.media_urls.append(burst.media_urls[0] + ".unrecorded.jpg")
        self.mp.setattr(self.adapter, "_drop_delayed_deliveries", True)
        await self.app.process_update(media_upd(self.app.bot, 8141, "a blue car", mid=5141))
        await self.app.process_update(media_upd(self.app.bot, 8142, "a green car", mid=5141, edited=True))
        self.assertEqual(burst.text, "a red car", "the held photo's notice never joins another photo's burst")
        await self.teardown_and_reconnect()
        await drain_media(self.adapter)
        texts = [e.text for e in self.delivered]
        notice = caption_notice(5141, "a blue car", "a green car")
        self.assertEqual(texts.count(notice), 1, texts)
        self.assertLess(texts.index("a blue car"), texts.index(notice), "the notice goes after its photo")

    # -- Which media Hermes batches with photos, and declined steers ---------------------------------------

    async def test_a_pdf_never_takes_a_waiting_bursts_unknown_path(self):
        """Only photos and image documents are merged into a photo burst: a PDF sent while the
        chat's burst holds one path no record knows (media sent before the caption handlers were wired) goes
        straight to a turn, so its caption edit's notice goes on its own and never joins the burst."""
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8200, "a red car", mid=5200))
        [burst] = self.adapter._pending_photo_batches.values()
        burst.media_urls.append(burst.media_urls[0] + ".unrecorded.jpg")
        await self.app.process_update(media_upd(self.app.bot, 8201, "the report", mid=5201, kind="document"))
        self.assertEqual([e.text for e in self.delivered], ["the report"], "straight to a turn")
        await self.app.process_update(media_upd(self.app.bot, 8202, "the Q3 report", mid=5201, kind="document",
                                                edited=True))
        self.assertEqual(burst.text, "a red car", "the PDF's notice never joins the photo burst")
        self.assertEqual([e.text for e in self.delivered][1:],
                         [caption_notice(5201, "the report", "the Q3 report", kind="file")])
        await drain_media(self.adapter)

    async def test_an_image_document_known_only_by_its_mime_type_is_found_in_its_burst(self):
        """Hermes batches a document named without an extension but typed image/png with the chat's photos
        (adapter._cache_inbound_document reads the mime type too): its caption edit is appended
        there, so the burst goes out once with the correction."""
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8210, "first", mid=5210))
        await self.app.process_update(media_upd(self.app.bot, 8211, "a screenshot", mid=5211,
                                                kind="nameless image document"))
        [burst] = self.adapter._pending_photo_batches.values()
        self.assertEqual(len(burst.media_urls), 2, "Hermes batched the image document with the photo")
        await self.app.process_update(media_upd(self.app.bot, 8212, "the screenshot", mid=5211,
                                                kind="nameless image document", edited=True))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered], [
            "first\n\na screenshot\n" + caption_notice(5211, "a screenshot", "the screenshot", kind="file")])

    async def test_a_caption_edit_of_a_voice_note_queued_after_a_declined_steer_reaches_its_turn(self):
        """Steer mode: Hermes transcribes a voice note for the steer, and when the running
        agent doesn't take it, queues the note with that transcript cached on it (run_busy.py:605-623, 862-863).
        The drain reads the cache, not .text (the interrupt-mode case is above)."""
        self.media()
        runner = BusyRunner(self.adapter)
        clips, echoes = with_stt(runner)
        runner.steering = runner.steer_declined = True
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 8220, "call mum", mid=5220, kind="voice"))
            await drain_media(self.adapter)
            queued = self.adapter._pending_messages[key]
            self.assertEqual((queued.text, len(clips), runner.steered), ("call mum", 1, []))
            self.assertTrue(hasattr(queued, "_gateway_pending_stt_text"), "Hermes cached the transcript")
            await self.app.process_update(media_upd(self.app.bot, 8221, "call dad", mid=5220, kind="voice",
                                                    edited=True))
            await drain_media(self.adapter)
            self.assertEqual(self.delivered, [])
            event, pending = await runner.next_turn(key, queued.source)
            self.assertIs(event, queued)
            self.assertEqual(pending, '"spoken 2"\n\ncall mum\n' + caption_notice(5220, "call mum", "call dad",
                                                                                   kind="voice message"))
            self.assertEqual(echoes, ["spoken 1"], "transcribed again, never echoed twice")

    # -- A correction gets no busy acknowledgement ------------------------------------------------------------

    async def test_an_edit_steered_into_the_turn_its_message_started_gets_no_busy_acknowledgement(self):
        """A message starts a turn, and while it runs the owner edits it. The correction goes into the
        running turn without Hermes' busy acknowledgement ("⏩ Steered into the current run ...", run_busy.py:882-905),
        which reads as a new message; a real message after it is acknowledged as before."""
        await self.app.process_update(upd(self.app.bot, 8230, "book the 9:00 train", mid=5230))
        await drain(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["book the 9:00 train"])     # its turn started
        runner = BusyRunner(self.adapter)
        runner.steering = True                    # the owner's busy_input_mode: steer
        with self.busy(runner) as key:
            turn = runner._session_state(key).turn
            await self.app.process_update(upd(self.app.bot, 8231, "book the 10:00 train", edited=True, mid=5230))
            await drain(self.adapter)
            [steered] = runner.steered
            self.assertTrue(steered.text.startswith(MARK))
            self.assertTrue(steered.text.endswith(corrected("book the 10:00 train")))
            self.assertEqual(runner.acks, [], "no busy acknowledgement for a correction")
            self.assertEqual(turn.busy_ack_ts, 0.0, "the turn's last-acknowledged time is back")
            await self.app.process_update(upd(self.app.bot, 8232, "and a taxi", mid=5232))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.acks], ["and a taxi"], "a real message is acknowledged")

    async def test_an_edit_leaves_hermes_own_30_second_acknowledgement_window_as_it_was(self):
        """Inside 30 s of an acknowledgement Hermes sends none (run_busy.py:888-891). A correction in between neither
        adds one nor moves that window: a real message inside it gets none, and one after it gets its own."""
        runner = BusyRunner(self.adapter)
        runner.steering = True
        with self.busy(runner) as key:
            await self.app.process_update(upd(self.app.bot, 8235, "check the oil", mid=5235))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.acks], ["check the oil"])
            turn = runner._session_state(key).turn
            acked = turn.busy_ack_ts
            await self.app.process_update(upd(self.app.bot, 8236, "check the tyres", edited=True, mid=5235))
            await drain(self.adapter)
            self.assertEqual(len(runner.steered), 2, "the correction went into the running turn")
            self.assertEqual(turn.busy_ack_ts, acked, "Hermes' window is still the one its acknowledgement opened")
            await self.app.process_update(upd(self.app.bot, 8237, "and the lights", mid=5237))
            await drain(self.adapter)
            self.assertEqual(len(runner.acks), 1, "inside Hermes' 30 s: none")
            turn.busy_ack_ts -= 31                # half a minute on
            await self.app.process_update(upd(self.app.bot, 8238, "and the wipers", edited=True, mid=5237))
            await drain(self.adapter)
            self.assertEqual(len(runner.steered), 4)
            self.assertEqual(len(runner.acks), 1, "nor for a correction after it")
            await self.app.process_update(upd(self.app.bot, 8239, "and the horn", mid=5239))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.acks], ["check the oil", "and the horn"])

    async def test_a_caption_edit_steered_into_a_busy_turn_gets_no_busy_acknowledgement(self):
        """The same for a caption: the photo started its turn, and the caption correction goes into it without the
        bubble."""
        self.media()
        await self.app.process_update(media_upd(self.app.bot, 8245, "a red car", mid=5245))
        await drain_media(self.adapter)
        self.assertEqual([e.text for e in self.delivered], ["a red car"])
        runner = BusyRunner(self.adapter)
        runner.steering = True
        with self.busy(runner) as key:
            await self.app.process_update(media_upd(self.app.bot, 8246, "a blue car", mid=5245, edited=True))
            await drain(self.adapter)
            self.assertEqual([e.text for e in runner.steered], [caption_notice(5245, "a red car", "a blue car")])
            self.assertEqual(runner.acks, [], "no busy acknowledgement for a caption correction")
            self.assertEqual(runner._session_state(key).turn.busy_ack_ts, 0.0)

    async def test_hermes_acknowledges_a_busy_message_at_most_once_per_30_seconds(self):
        """Parity for BusyRunner._ack and the plugin's hush: Hermes' own busy handler (run_busy.py:807-905), run on a
        bare runner whose routing steers, sends its acknowledgement only when the turn's busy_ack_ts is 30 s old or
        more, and stamps it."""
        class Bare(BusyRunner):
            def _is_user_authorized_for_source(self, source):
                return True

            def _admit_bot_message_for_source(self, source):
                return True

            def _effective_busy_input_mode(self, source):
                return "steer"

            def _effective_busy_text_mode(self, source):
                return "interrupt"

            async def _route_plaintext_approval_while_busy(self, event, session_key):
                return False

            async def _resolve_busy_steer_or_redirect(self, event, session_key, effective_mode, running_agent):
                return GatewayRunner._BusySteerOutcome(effective_mode="steer", demoted_for_subagents=False,
                                                       demoted_for_compression=False, steered=True, redirected=False)

            def _busy_steer_ack_enabled(self, event, session_key):
                return True

            def _compose_busy_ack_message(self, event, now, _busy_state, running_agent, **modes):
                return "⏩ Steered into the current run"

            async def _send_busy_ack_reply(self, event, adapter, message):
                self.acks.append(message)

        runner = Bare(self.adapter)
        event = self.adapter._build_message_event(upd(self.app.bot, 8250, "hello", mid=5250).message,
                                                  T.MessageType.TEXT)
        key = self.adapter._event_session_key(event)
        turn = runner._session_state(key).turn
        handle = GatewayBusySessionMixin._handle_active_session_busy_message
        self.assertTrue(await handle(runner, event, key))
        self.assertEqual(runner.acks, ["⏩ Steered into the current run"])
        self.assertGreater(turn.busy_ack_ts, time.time() - 5)
        turn.busy_ack_ts = time.time() - 29
        self.assertTrue(await handle(runner, event, key))
        self.assertEqual(len(runner.acks), 1, "29 s after one: none")
        turn.busy_ack_ts = time.time() - 31
        self.assertTrue(await handle(runner, event, key))
        self.assertEqual(len(runner.acks), 2, "31 s after one: acknowledged again")

if __name__ == "__main__":
    unittest.main(verbosity=2)
