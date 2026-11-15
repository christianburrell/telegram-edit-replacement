import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock
from datetime import datetime, timezone
from types import SimpleNamespace as NS

HERE = Path(__file__).resolve().parents[1]
ROOT = Path(os.environ.get("EDIT_PLUGIN_DIR", HERE / "plugin"))
MARK = "[Telegram edit:"
NOT_RUN = "it was not run"
LOGGER = "hermes.plugins.telegram_edit_replacement"
PTB = "python-telegram-bot"
COMMIT = "d923a5eb487a17c0a68801bba9ef0f56c81040a8"


def load_pin_script():
    spec = importlib.util.spec_from_file_location("pin_hermes_for_tests", HERE / "scripts/pin_hermes.py")
    assert spec is not None and spec.loader is not None
    pin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pin)
    return pin


def load_plugin(verified=True):
    """The plugin module; verified=True stands in a Hermes that matches its pin (PinTests cover the
    check itself: this interpreter may have no Hermes at all)."""
    spec = importlib.util.spec_from_file_location("edit_replacement", ROOT / "__init__.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    if verified:
        mod.pin_mismatches = lambda pin, root: []
    return mod


def owner_message():
    return NS(from_user=NS(id=88), chat=NS(id=88, type="private"), via_bot=None)


def event(text="corrected wording", edited=True):
    return NS(text=text, internal=False, allow_gateway_control=True,
        message_id="472", metadata={},
        source=NS(platform=NS(value="telegram"), user_id="88", chat_id="88",
                  chat_type="dm", is_bot=False, profile="default", thread_id="9001"),
        raw_message=NS(message_id=472, text=text, chat=NS(id=88),
            from_user=NS(id=88, is_bot=False),
            edit_date=datetime(2001, 2, 3, 20, tzinfo=timezone.utc) if edited else None,
            date=datetime(2001, 2, 3, 19, tzinfo=timezone.utc)))


class Ctx:
    """The PluginContext surface register() uses."""
    plugin_id = "telegram-edit-replacement"

    def __init__(self, profile, owner: object = "88"):
        self.profile_name = profile
        self.owner = owner
        self.factories = []
        self.unload = []

    def get_config(self, key, default=None):
        return self.owner if key == "owner_user_id" else default

    def register_telegram_handler(self, callback):
        self.factories.append(callback)

    def on_unload(self, callback):
        self.unload.append(callback)


@contextlib.contextmanager
def home(test):
    """A throwaway HERMES_HOME for the duration; yields the plugin's state key for it."""
    path = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))
    test.addCleanup(shutil.rmtree, path, True)
    old = os.environ.get("HERMES_HOME")
    os.environ["HERMES_HOME"] = path
    try:
        yield load_plugin().state_key()
    finally:
        if old is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = old


class Gate:
    def __init__(self, value=True):
        self.value = value

    def enabled(self):
        return self.value


class ReplacementTests(unittest.TestCase):
    def test_genuine_edit_is_a_replacement_not_a_repeat(self):
        mod = load_plugin()
        e = event()
        r = mod.ReplacementPolicy("88").on_message(event=e)
        self.assertIsInstance(r, dict)
        self.assertEqual(r["action"], "rewrite")
        text = r["text"]
        self.assertTrue(text.startswith(MARK))
        for phrase in ("message 472", "2001-02-03T19:00:00+00:00", "ignore the earlier wording",
                       "including if it reaches you after this note",
                       "not a repeated or additional request", "other messages still stand",
                       "say so briefly instead of repeating work", "ask before undoing",
                       "if you never saw the earlier version, treat the corrected text as a new message",
                       "not a slash command or an approval",
                       "anything after the end line is a separate message"):
            self.assertIn(phrase, text)
        self.assertNotIn("Earlier wording (void)", text)
        self.assertIn("\nEarlier wording: not available", text)
        self.assertIn("nothing changed: don't redo it", text)
        self.assertTrue(text.endswith("\nCorrected text:\ncorrected wording\n" + mod.END_LINE))
        self.assertFalse(e.allow_gateway_control)
        self.assertEqual(e.text, "corrected wording")

    def test_earlier_wording_is_quoted_on_one_line_and_capped(self):
        mod = load_plugin()
        r = mod.ReplacementPolicy("88").on_message(event=event(), earlier="meet at\n  5pm")
        self.assertIn("\nEarlier wording (void): «meet at 5pm»\nCorrected text:\ncorrected wording\n" + mod.END_LINE,
                      r["text"])
        self.assertNotIn("not available", r["text"])
        r = mod.ReplacementPolicy("88").on_message(event=event(), earlier="x" * (mod.EARLIER_LIMIT + 50))
        quoted = r["text"].split("«", 1)[1].split("»", 1)[0]
        self.assertEqual(len(quoted), mod.EARLIER_LIMIT)
        self.assertTrue(quoted.endswith("…"))
        r = mod.ReplacementPolicy("88").on_message(event=event(), earlier="y" * mod.EARLIER_LIMIT)
        self.assertIn("«" + "y" * mod.EARLIER_LIMIT + "»", r["text"])
        for blank in ("", "   ", None, 5):
            with self.subTest(earlier=blank):
                r = mod.ReplacementPolicy("88").on_message(event=event(), earlier=blank)
                self.assertNotIn("Earlier wording (void)", r["text"])
                self.assertIn("Earlier wording: not available", r["text"])

    def test_a_correction_that_reads_as_a_command_is_said_not_to_have_run(self):
        mod = load_plugin()
        plain = mod.ReplacementPolicy("88").notice(event(), "earlier")
        command = mod.ReplacementPolicy("88").notice(event(), "earlier", command=True)
        self.assertNotIn(NOT_RUN, plain)
        self.assertIn(NOT_RUN, command)
        self.assertIn("Don't carry it out yourself", command)
        self.assertIn("ask them to send it again as a new message", command)
        for text in (plain, command):
            self.assertTrue(text.startswith(MARK))
            self.assertTrue(text.endswith("\nCorrected text:\ncorrected wording\n" + mod.END_LINE))

    def test_message_log_keeps_latest_and_is_bounded(self):
        mod = load_plugin()

        def seen(text):
            return mod.Seen(text, mod.fingerprint(text))

        store = {}
        cache = mod.WordingCache(store=store, max_entries=2)
        cache.put(88, 1, seen("one"))
        cache.put(88, 2, seen("two"))
        cache.put(88, 1, seen("one again"))
        cache.put(88, 3, seen("three"))
        self.assertIsNone(cache.get(88, 2))
        self.assertEqual(cache.get(88, 1).wording, "one again")
        self.assertEqual(cache.get("88", "3").wording, "three")
        self.assertIsNone(cache.get(99, 3))
        long_text = "y" * (mod.STORE_LIMIT + 10)
        cache.put(88, 4, seen(long_text))
        self.assertEqual(len(cache.get(88, 4).wording), mod.STORE_LIMIT)
        self.assertEqual(cache.get(88, 4).fingerprint, mod.fingerprint(long_text), "the whole text decides 'unchanged'")
        self.assertNotEqual(mod.fingerprint(long_text), mod.fingerprint(long_text[:mod.STORE_LIMIT]))
        for bad in ("", None, 7):
            with self.subTest(text=bad):
                cache.put(88, 5, mod.Seen(bad, "x"))
                self.assertIsNone(cache.get(88, 5))
        self.assertGreater(mod.WordingCache().max_entries, 0)
        # The data lives in the given store, so a reloaded module keeps what the old one saw.
        self.assertEqual(mod.WordingCache(store=store).get(88, 3).wording, "three")

    def test_only_real_owner_dm_edits_are_rewritten(self):
        mod = load_plugin()
        cases = [
            ("ordinary message", lambda e: setattr(e.raw_message, "edit_date", None)),
            ("fake date", lambda e: setattr(e.raw_message, "edit_date", "yes edited")),
            ("internal", lambda e: setattr(e, "internal", True)),
            ("non Telegram", lambda e: setattr(e.source.platform, "value", "discord")),
            ("other user", lambda e: setattr(e.source, "user_id", "99")),
            ("other chat", lambda e: setattr(e.source, "chat_id", "99")),
            ("group", lambda e: setattr(e.source, "chat_type", "group")),
            ("source bot", lambda e: setattr(e.source, "is_bot", True)),
            ("raw bot", lambda e: setattr(e.raw_message.from_user, "is_bot", True)),
            ("raw different user", lambda e: setattr(e.raw_message.from_user, "id", 99)),
            ("raw different chat", lambda e: setattr(e.raw_message.chat, "id", 99)),
            ("mismatched message", lambda e: setattr(e.raw_message, "message_id", 99)),
            ("no raw", lambda e: setattr(e, "raw_message", None)),
            ("blank", lambda e: setattr(e, "text", "")),
            ("non string", lambda e: setattr(e, "text", None)),
            ("number text", lambda e: setattr(e, "text", 5)),
            ("bad id", lambda e: setattr(e, "message_id", "472] injected")),
            ("oversized id", lambda e: setattr(e, "message_id", "4" * 1000)),
            ("wrong profile", lambda e: setattr(e.source, "profile", "another")),
            ("oversized matching id", lambda e: (setattr(e, "message_id", "4" * 30),
                                                 setattr(e.raw_message, "message_id", int("4" * 30)))),
            ("non-numeric matching id", lambda e: (setattr(e, "message_id", "47a"),
                                                   setattr(e.raw_message, "message_id", "47a"))),
        ]
        for name, change in cases:
            with self.subTest(name=name):
                e = event()
                change(e)
                before = (e.text, e.allow_gateway_control)
                self.assertIsNone(mod.ReplacementPolicy("88").on_message(event=e, earlier="old"))
                self.assertEqual((e.text, e.allow_gateway_control), before)

    def test_owner_ids_are_normalised(self):
        mod = load_plugin()
        self.assertEqual(mod.numeric_id("0088"), "88")
        self.assertEqual(mod.numeric_id(88), "88")

        class DigitsStr:
            def __str__(self):
                return "88"

        self.assertIsNone(mod.numeric_id(DigitsStr()), "ids are str or int only")
        self.assertIsNotNone(mod.ReplacementPolicy("0088").on_message(event=event()))

    def test_enabled_rule_mirrors_hermes(self):
        mod = load_plugin()
        key = "telegram-edit-replacement"
        cases = [
            ({"plugins": {"enabled": [key]}}, True),
            ({"plugins": {"enabled": [key], "disabled": [key]}}, False),
            ({"plugins": {"enabled": ["other"]}}, False),
            ({"plugins": {"enabled": []}}, False),
            ({"plugins": {"enabled": key}}, False),
            ({"plugins": {}}, False),
            ({"plugins": None}, False),
            ({}, False),
            (None, False),
            ({"plugins": {"enabled": [key], "disabled": "x"}}, True),
            ({"plugins": {"enabled": [key], "disabled": [{"odd": 1}, key]}}, True),
            ({"plugins": {"enabled": [{"odd": 1}, key]}}, False),
            ({"plugins": {"enabled": {key: True}}}, False),
            ({"plugins": {"enabled": [key], "disabled": {key: True}}}, True),
        ]
        for cfg, expected in cases:
            with self.subTest(cfg=cfg):
                self.assertIs(mod.plugin_enabled(cfg, key), expected)

    def test_unconfigured_owner_never_matches_missing_identities(self):
        e = event()
        e.source.user_id = e.source.chat_id = None
        e.raw_message.from_user.id = None
        e.raw_message.chat.id = None
        self.assertIsNone(load_plugin().ReplacementPolicy(None).on_message(event=e))

    def test_registration_swaps_the_shared_runtime_for_reloads(self):
        mod = load_plugin()
        with home(self) as key:
            first, second = Ctx("default"), Ctx("default")
            mod.register(first)
            shared = mod.shared_state(key)
            # No unload hook: Hermes unloads every plugin at the start of a forced reload, and
            # clearing then would leave edits untagged until the new load. Config turns it off.
            # Two factories: the text handlers' and, wired on their own, the caption handlers'.
            self.assertEqual((len(first.factories), len(first.unload)), (2, 0))
            old = shared.current
            self.assertIsNotNone(old)
            mod.register(second)  # a reload sweep loads the module again
            self.assertIsNot(shared.current, old)
            # a reloaded module object finds the same process-wide state
            self.assertIs(load_plugin().shared_state(key), shared)

    def test_each_hermes_home_keeps_its_own_state(self):
        mod = load_plugin()
        with home(self) as a:
            mod.register(Ctx("custom", owner="88"))
        with home(self) as b:
            mod.register(Ctx("custom", owner="99"))     # Hermes names every non-standard home 'custom'
        self.assertNotEqual(a, b)
        self.assertEqual(mod.shared_state(a).current.policy.owner, "88")
        self.assertEqual(mod.shared_state(b).current.policy.owner, "99")

    def test_a_registration_that_fails_never_takes_over(self):
        mod = load_plugin()
        with home(self) as key:
            mod.register(Ctx("default"))
            shared = mod.shared_state(key)
            before = shared.current
            broken = Ctx("default")

            def refuse(callback):
                raise RuntimeError("load timed out")

            broken.register_telegram_handler = refuse
            with self.assertRaises(RuntimeError):
                mod.register(broken)
            self.assertIs(shared.current, before)

    def test_a_fresh_gate_counts_as_enabled_until_the_file_says_otherwise(self):
        mod = load_plugin()
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        cfg = tmp / "config.yaml"
        cfg.write_text("plugins: {enabled: [telegram-edit-replacement]\n")     # does not parse
        self.assertTrue(mod.ConfigGate(cfg).enabled(), "Hermes loaded the plugin, so its config enabled it")
        self.assertFalse(mod.ConfigGate(tmp / "missing.yaml").enabled())

    def test_factory_name_is_stable_across_loads(self):
        # Hermes runs a factory once per Telegram client per (plugin, qualname): a stable name means
        # a reload never wires a second set of handlers.
        profile = "unit-name-" + str(id(self))
        first, second = Ctx(profile), Ctx(profile)
        load_plugin().register(first)
        load_plugin().register(second)
        self.assertEqual(first.factories[0].__qualname__, second.factories[0].__qualname__)
        self.assertEqual(first.factories[1].__qualname__, second.factories[1].__qualname__)
        self.assertNotEqual(first.factories[0].__qualname__, first.factories[1].__qualname__,
                            "Hermes keys each factory by its name: the caption one must be its own")

    def test_missing_owner_is_reported_at_load(self):
        mod = load_plugin()
        for owner in (None, "not-an-id"):
            with self.subTest(owner=owner):
                with self.assertLogs(LOGGER, "WARNING") as logs:
                    mod.register(Ctx("unit-owner-" + str(id(self)), owner=owner))
                self.assertTrue(any("owner_user_id" in line for line in logs.output))
        with self.assertNoLogs(LOGGER, "WARNING"):
            mod.register(Ctx("unit-owner-" + str(id(self)), owner="88"))

    def test_live_handlers_accept_only_the_owners_dm_while_enabled(self):
        mod = load_plugin()
        state = mod.shared_state("unit-accept-" + str(id(self)))

        def msg():
            return NS(from_user=NS(id=88), chat=NS(id=88, type="private"), via_bot=None)

        live = mod.Generation(state, mod.ReplacementPolicy("88"), Gate())
        self.assertTrue(live.accepts_message(msg()))
        cases = {
            "other user": lambda m: setattr(m.from_user, "id", 99),
            "other chat": lambda m: setattr(m.chat, "id", 99),
            "not a private chat": lambda m: setattr(m.chat, "type", "group"),
            "sent via an inline bot": lambda m: setattr(m, "via_bot", NS(id=5)),
            "no sender": lambda m: setattr(m, "from_user", None),
        }
        for name, change in cases.items():
            with self.subTest(name):
                m = msg()
                change(m)
                self.assertFalse(live.accepts_message(m))
        disabled = mod.Generation(state, mod.ReplacementPolicy("88"), Gate(False))
        self.assertFalse(disabled.accepts_message(msg()))
        anonymous = NS(from_user=NS(id=None), chat=NS(id=None, type="private"), via_bot=None)
        self.assertFalse(mod.Generation(state, mod.ReplacementPolicy(None), Gate()).accepts_message(anonymous))

    def test_a_load_on_an_unverified_hermes_stays_inert_and_says_why(self):
        mod = load_plugin(verified=False)
        mod.pin_mismatches = lambda pin, root: ["gateway/run_busy.py"]
        with home(self) as key:
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            generation = mod.shared_state(key).current
            generation.gate = Gate(True)
            self.assertFalse(generation.accepts_message(owner_message()))
        joined = "\n".join(logs.output)
        self.assertIn("WARNING", joined)
        self.assertIn("gateway/run_busy.py", joined)
        self.assertRegex(joined, r"loaded \(pid \d+, inactive: Hermes differs from the version")

    def test_a_verified_load_is_active_and_says_so(self):
        mod = load_plugin()
        with home(self) as key:
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            generation = mod.shared_state(key).current
            generation.gate = Gate(True)
            self.assertTrue(generation.accepts_message(owner_message()))
        self.assertRegex("\n".join(logs.output), r"telegram-edit-replacement loaded \(pid \d+, active\)")

    def test_a_load_without_an_owner_says_it_is_inactive(self):
        mod = load_plugin()
        with home(self):
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default", owner=None))
        self.assertRegex("\n".join(logs.output), r"loaded \(pid \d+, inactive: no owner_user_id\)")

    def test_register_checks_its_own_pin_against_the_running_hermes(self):
        mod = load_plugin(verified=False)
        calls = []
        mod.pin_mismatches = lambda pin, root: calls.append((pin, root)) or []
        mod.hermes_source_root = lambda: Path("/hermes-checkout")
        with home(self) as key:
            mod.register(Ctx("default"))
            gate = mod.shared_state(key).current.gate
        self.assertEqual(calls, [(ROOT.resolve() / "verified_hermes.json", Path("/hermes-checkout"))])
        self.assertEqual(gate.manifest, ROOT.resolve() / "plugin.yaml", "it watches its own directory")

    def test_changed_handler_code_asks_for_a_gateway_restart(self):
        mod = load_plugin()
        self.assertRegex(mod.WIRING_DIGEST or "", r"^[0-9a-f]{64}$")

        class Native:
            pass

        native = Native()
        with home(self) as key:
            state = mod.shared_state(key)
            state.wired[native] = "the handler code of an older version"
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            joined = "\n".join(logs.output)
            self.assertIn("WARNING", joined)
            self.assertRegex(joined, r"loaded \(pid \d+, active; restart the gateway")
            state.wired[native] = mod.WIRING_DIGEST
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            self.assertNotIn("restart", "\n".join(logs.output))

    def test_the_restart_check_covers_exactly_the_code_wired_once(self):
        """The thin handlers (_wire), the fallback and logging they call, and the recorder's PTB group are
        fixed at first wiring: a change to any of them needs a restart, a change elsewhere does not."""
        source = (ROOT / "__init__.py").read_text()
        base = load_plugin().WIRING_DIGEST
        self.assertRegex(base or "", r"^[0-9a-f]{64}$")

        def digest_with(old, new):
            self.assertEqual(source.count(old), 1, old)
            tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
            (tmp / "__init__.py").write_text(source.replace(old, new))
            spec = importlib.util.spec_from_file_location("edit_replacement_variant", tmp / "__init__.py")
            assert spec is not None and spec.loader is not None
            variant = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = variant
            spec.loader.exec_module(variant)
            return variant.WIRING_DIGEST

        for where, old, new in (
                ("_wire", "    if native in state.wired:\n        return\n",
                 "    if native in state.wired:\n        return None\n"),
                ("deliver_like_core", "        await adapter._handle_command(update, context)\n",
                 "        await adapter._handle_command(update, context)   # changed\n"),
                ("_log_failure", 'else "?"\n', 'else "unknown"\n'),
                ("RECORDER_GROUP", "RECORDER_GROUP = 97 ", "RECORDER_GROUP = 96 ")):
            with self.subTest(changed=where):
                self.assertNotEqual(digest_with(old, new), base)
        self.assertEqual(digest_with('"end line is a separate message.]"', '"end line is a separate message!]"'),
                         base, "the notice reloads with the code: no restart")

    def test_a_handler_fingerprint_that_cannot_be_read_never_breaks_the_wiring(self):
        mod = load_plugin()
        for exc in (OSError("gone"), TypeError("built-in"), SyntaxError("garbled"), IndentationError("x"),
                    ValueError("x")):
            with self.subTest(exc=type(exc).__name__):
                with mock.patch("inspect.getsource", side_effect=exc):
                    self.assertIsNone(mod.wiring_digest())

    def test_a_removed_plugin_directory_reads_as_disabled(self):
        mod = load_plugin()
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        cfg = tmp / "config.yaml"
        cfg.write_text("plugins:\n  enabled: [telegram-edit-replacement]\n")
        manifest = tmp / "plugin.yaml"
        manifest.write_text("name: telegram-edit-replacement\n")
        gate = mod.ConfigGate(cfg, manifest=manifest)
        self.assertTrue(gate.enabled())
        manifest.unlink()
        self.assertFalse(gate.enabled(), "Hermes would not load it again: hand edits back at once")


    def test_changed_caption_handler_code_asks_for_a_gateway_restart(self):
        mod = load_plugin()
        self.assertRegex(mod.CAPTION_WIRING_DIGEST or "", r"^[0-9a-f]{64}$")

        class Native:
            pass

        native = Native()
        with home(self) as key:
            state = mod.shared_state(key)
            mod._caption_wired(state)[native] = "the caption handler code of an older version"
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            self.assertRegex("\n".join(logs.output), r"loaded \(pid \d+, active; restart the gateway")
            mod._caption_wired(state)[native] = mod.CAPTION_WIRING_DIGEST
            with self.assertLogs(LOGGER, "INFO") as logs:
                mod.register(Ctx("default"))
            self.assertNotIn("restart", "\n".join(logs.output))

    def test_the_text_handlers_wiring_is_unchanged_since_before_captions(self):
        """Hot install: the caption handlers come with their own factory, so the text handlers' wiring is
        byte-for-byte that of the version before captions, and no restart is asked for."""
        self.assertEqual(load_plugin().WIRING_DIGEST,
                         "ead85dc1bd4155989754f3a18c2616681a3fba8d61dbeaf556bb74afd7c62dd9")

    def test_the_caption_restart_check_covers_exactly_the_caption_wiring(self):
        source = (ROOT / "__init__.py").read_text()
        base = load_plugin()
        self.assertRegex(base.CAPTION_WIRING_DIGEST or "", r"^[0-9a-f]{64}$")

        def digests_with(old, new):
            self.assertEqual(source.count(old), 1, old)
            tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
            (tmp / "__init__.py").write_text(source.replace(old, new))
            spec = importlib.util.spec_from_file_location("edit_replacement_caption_variant", tmp / "__init__.py")
            assert spec is not None and spec.loader is not None
            variant = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = variant
            spec.loader.exec_module(variant)
            return variant.WIRING_DIGEST, variant.CAPTION_WIRING_DIGEST

        for where, old, new in (
                ("_wire_captions", "    if native in wired:\n        return\n", "    if native in wired:\n        return None\n"),
                ("_log_failure", 'else "?"\n', 'else "unknown"\n'),
                ("MEDIA_RECORDER_GROUP", "MEDIA_RECORDER_GROUP = 98 ", "MEDIA_RECORDER_GROUP = 96 ")):
            with self.subTest(changed=where):
                self.assertNotEqual(digests_with(old, new)[1], base.CAPTION_WIRING_DIGEST)
        self.assertEqual(digests_with("    if native in wired:\n        return\n",
                                      "    if native in wired:\n        return None\n")[0], base.WIRING_DIGEST,
                         "a caption wiring change never asks to restart for the text handlers")
        self.assertEqual(digests_with('"line is a separate message.]"', '"line is a separate message!]"')[1],
                         base.CAPTION_WIRING_DIGEST, "the caption notice reloads with the code: no restart")
        with mock.patch("inspect.getsource", side_effect=OSError("gone")):
            self.assertIsNone(base.caption_wiring_digest())

    def caption_adapter(self, mod, *, batches=(), slots=(), fifo=()):
        return NS(_pending_text_batches={}, _text_debounce_store=lambda: {},
                  _pending_photo_batches={f"b{i}": e for i, e in enumerate(batches)}, _media_group_events={},
                  _pending_messages={f"s{i}": e for i, e in enumerate(slots)},
                  gateway_runner=NS(_overflow_queue=lambda key: list(fifo)), _event_session_key=lambda e: "s0")

    def test_a_new_media_message_is_found_by_its_own_event_first(self):
        mod = load_plugin()
        msg = NS()
        own = NS(raw_message=msg, media_urls=["/c/new.jpg"])
        other = NS(raw_message=NS(), media_urls=["/c/unknown.jpg"])
        adapter = self.caption_adapter(mod, slots=[other], fifo=[own])
        self.assertEqual(mod._media_carrier(adapter, NS(), msg, set(), None), (own, "/c/new.jpg"))

    def test_a_merged_media_message_is_the_one_path_no_record_knows_in_the_batch_it_merged_into(self):
        mod = load_plugin()
        burst = NS(raw_message=NS(), media_urls=["/c/a.jpg", "/c/b.jpg"])
        slot = NS(raw_message=NS(), media_urls=["/c/old.jpg"])
        adapter = self.caption_adapter(mod, batches=[burst], slots=[slot])
        self.assertEqual(mod._media_carrier(adapter, NS(), NS(), {"/c/old.jpg"}, burst), (None, None),
                         "two paths no record knows in its batch: never a guess")
        self.assertEqual(mod._media_carrier(adapter, NS(), NS(), {"/c/a.jpg"}, burst), (burst, "/c/b.jpg"),
                         "only the batch it merged into counts: the slot's unknown path doesn't")
        self.assertEqual(mod._media_carrier(adapter, NS(), NS(), {"/c/a.jpg", "/c/old.jpg"}, burst),
                         (burst, "/c/b.jpg"))
        self.assertEqual(mod._media_carrier(adapter, NS(), NS(), {"/c/a.jpg", "/c/b.jpg"}, burst), (None, None),
                         "an unknown path elsewhere (the slot) is never taken")
        self.assertEqual(mod._media_carrier(adapter, NS(), NS(), {"/c/a.jpg", "/c/b.jpg"}, None), (None, None),
                         "merged nowhere: it takes no waiting path")

    def test_only_photos_and_image_documents_are_merged_into_a_photo_burst(self):
        """Hermes routes only photos and image documents through its photo burst (_handle_media_message ->
        _route_photo_event; _cache_inbound_document); other kinds outside an album reach handle_message as their
        own events, so a burst waiting in the same chat is never where they were merged."""
        mod = load_plugin()
        burst = NS(raw_message=NS(), media_urls=["/c/a.jpg"])
        adapter = NS(_media_group_events={}, _pending_photo_batches={"k": burst},
                     _photo_batch_key=lambda keyed, msg: "k")
        photo = NS(media_group_id=None, photo=[NS()], document=None)
        self.assertIs(mod._merged_batch(adapter, NS(), photo), burst, "positive control")
        for kind in ("video", "voice", "audio", "video_note", "sticker"):
            with self.subTest(kind=kind):
                msg = NS(media_group_id=None, photo=None, document=None, **{kind: NS()})
                self.assertIsNone(mod._merged_batch(adapter, NS(), msg))

    def test_media_records_without_a_caption_are_kept_and_known_paths_come_from_them(self):
        mod = load_plugin()
        gen = mod.Generation(mod.shared_state("unit-media-" + str(id(self))), mod.ReplacementPolicy("88"), Gate())
        gen.cache.put(88, 1, mod.SeenMedia("", mod.fingerprint(""), media=("photo", "u1", "/c/1.jpg")))
        gen.cache.put(88, 2, mod.SeenMedia("x", mod.fingerprint("x"), media=("photo", "u2", None)))
        gen.cache.put(88, 3, mod.Seen("", mod.fingerprint("")))
        self.assertIsNotNone(gen.cache.get(88, 1), "a photo sent without a caption is remembered")
        self.assertIsNone(gen.cache.get(88, 3), "a text record still needs words")
        self.assertEqual(gen._known_paths(), {"/c/1.jpg"})

    def test_a_caption_notice_names_the_kind_and_quotes_a_long_earlier_caption_shortened(self):
        mod = load_plugin()
        policy = mod.ReplacementPolicy("88")
        long = "word " * 200
        text = policy.caption_notice(event(), "audio file", long, "short")
        self.assertIn("message 472, an audio file (sent 2001-02-03T19:00:00+00:00)", text)
        self.assertIn("The audio file itself did not change", text)
        self.assertIn("Earlier caption (void): «" + ("word " * 100)[:499] + "…»", text)
        self.assertIn("message 472, a GIF (sent", policy.caption_notice(event(), "GIF", "", "x"))

    def test_a_removed_caption_is_an_edit_the_policy_accepts(self):
        policy = load_plugin().ReplacementPolicy("88")
        removed = event(text="")
        self.assertTrue(policy.accepts_caption(removed))
        self.assertEqual(removed.text, "", "the edit's own event is never changed")
        self.assertFalse(policy.accepts(removed), "the text path still needs text")
        self.assertFalse(policy.accepts_caption(event(text=None)))
        stranger = event(text="")
        stranger.source.user_id = "99"
        self.assertFalse(policy.accepts_caption(stranger), "every rule of accepts() still applies")


class HushTests(unittest.TestCase):
    """_hush_busy_ack: Hermes answers a message that reaches a busy chat with its busy acknowledgement at most once per
    30 s of a turn (gateway/run_busy.py:888-896, the turn's busy_ack_ts). A correction is no new message, so its
    dispatch runs with that time set to now; the undo puts the old time back."""

    def test_the_dispatch_runs_with_the_turn_just_acknowledged_and_the_undo_puts_the_time_back(self):
        # Both keys: the adapter's (base.handle_message passes it) and the runner's (a multiplexed gateway's busy
        # handler recomputes it, run_adapters.py:1745-1760).
        mod = load_plugin()
        turns = {"adapter-key": NS(busy_ack_ts=100.0), "runner-key": NS(busy_ack_ts=0.0)}
        runner = NS(_peek_session_state=lambda key: NS(turn=turns[key]) if key in turns else None,
                    _session_key_for_source=lambda source: "runner-key")
        before = time.time()
        undo = mod._hush_busy_ack(NS(gateway_runner=runner, _event_session_key=lambda e: "adapter-key"), event())
        for key, turn in turns.items():
            self.assertGreaterEqual(turn.busy_ack_ts, before, key)
            self.assertLessEqual(turn.busy_ack_ts, time.time(), key)
        undo()
        self.assertEqual({key: turn.busy_ack_ts for key, turn in turns.items()},
                         {"adapter-key": 100.0, "runner-key": 0.0})

    def test_the_undo_keeps_an_acknowledgement_hermes_sent_meanwhile(self):
        mod = load_plugin()
        turn = NS(busy_ack_ts=100.0)
        runner = NS(_peek_session_state=lambda key: NS(turn=turn), _session_key_for_source=lambda source: "k")
        undo = mod._hush_busy_ack(NS(gateway_runner=runner, _event_session_key=lambda e: "k"), event())
        turn.busy_ack_ts = 12345.0          # Hermes acknowledged a real message in between
        undo()
        self.assertEqual(turn.busy_ack_ts, 12345.0)

    def test_without_a_runner_or_a_session_it_changes_nothing_and_raises_nothing(self):
        mod = load_plugin()

        def boom(*args):
            raise RuntimeError("no such session")

        turn = NS(busy_ack_ts=7.0)
        for adapter in (NS(_event_session_key=lambda e: "k"),                                  # no runner
                        NS(gateway_runner=NS(_peek_session_state=lambda key: None,
                                             _session_key_for_source=lambda source: "k"),
                           _event_session_key=lambda e: "k"),                                  # no session yet
                        NS(gateway_runner=NS(_peek_session_state=boom, _session_key_for_source=boom),
                           _event_session_key=lambda e: "k"),
                        NS(gateway_runner=NS(_peek_session_state=lambda key: NS(turn=turn),
                                             _session_key_for_source=boom),
                           _event_session_key=boom)):
            with self.subTest(adapter=adapter):
                undo = mod._hush_busy_ack(adapter, event())
                undo()
        self.assertEqual(turn.busy_ack_ts, 7.0, "no key, no stamp")

    def test_the_runner_key_alone_is_enough(self):
        # A multiplexed gateway's busy handler reads the runner's key; the adapter's may name no session.
        mod = load_plugin()
        turn = NS(busy_ack_ts=0.0)
        runner = NS(_peek_session_state=lambda key: NS(turn=turn) if key == "runner-key" else None,
                    _session_key_for_source=lambda source: "runner-key")
        undo = mod._hush_busy_ack(NS(gateway_runner=runner, _event_session_key=lambda e: "adapter-key"), event())
        self.assertGreater(turn.busy_ack_ts, 0.0)
        undo()
        self.assertEqual(turn.busy_ack_ts, 0.0)


class PinTests(unittest.TestCase):
    """The plugin runs only on the Hermes it was verified on (verified_hermes.json), with the Telegram
    library it was verified with."""

    def setUp(self):
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        self.root = self.tmp / "hermes"
        (self.root / "gateway").mkdir(parents=True)
        (self.root / "gateway/run_busy.py").write_text("busy rules\n")
        (self.root / "gateway/base.py").write_text("base rules\n")
        self.pin = self.tmp / "verified_hermes.json"
        self.mod = load_plugin(verified=False)
        self.versions = {PTB: "22.5"}
        self.running = COMMIT          # the Hermes commit this process started from

    def write_pin(self, files, packages=None, commit=COMMIT):
        self.pin.write_text(json.dumps({"hermes_commit": commit, "files": files,
                                        "packages": {PTB: "22.5"} if packages is None else packages}))

    def mismatches(self, root, pin=None):
        return self.mod.pin_mismatches(pin or self.pin, root, version_of=self.versions.get,
                                       running_commit=lambda: self.running)

    def digest(self, rel):
        return hashlib.sha256((self.root / rel).read_bytes()).hexdigest()

    def test_matching_files_are_verified(self):
        self.write_pin({rel: self.digest(rel) for rel in ("gateway/run_busy.py", "gateway/base.py")})
        self.assertEqual(self.mismatches(self.root), [])

    def test_changed_and_missing_files_are_reported(self):
        good = self.digest("gateway/base.py")
        self.write_pin({"gateway/run_busy.py": "0" * 64, "gateway/base.py": good, "gateway/gone.py": good})
        self.assertEqual(self.mismatches(self.root), ["gateway/gone.py", "gateway/run_busy.py"])
        self.assertEqual(self.mismatches(None),
                         ["gateway/base.py", "gateway/gone.py", "gateway/run_busy.py"], "no Hermes found")

    def test_a_different_telegram_library_is_a_mismatch(self):
        # A venv dependency sync can change python-telegram-bot without touching a pinned Hermes file.
        self.write_pin({rel: self.digest(rel) for rel in ("gateway/run_busy.py", "gateway/base.py")})
        self.versions = {PTB: "22.6"}
        self.assertEqual(self.mismatches(self.root), [f"{PTB} 22.6"])
        self.versions = {}
        self.assertEqual(self.mismatches(self.root), [f"{PTB} (not installed)"])

    def test_a_library_that_is_not_installed_has_no_version(self):
        self.assertIsNone(self.mod._installed_version("no-such-distribution-edit-replacement"))

    def test_a_gateway_still_running_another_hermes_commit_is_a_mismatch(self):
        # A gateway keeps running the code it imported at startup: files updated (and pinned) on disk
        # since then are not what it runs (hermes_cli/version_info.py, gateway_state.json code_sha).
        self.write_pin({rel: self.digest(rel) for rel in ("gateway/run_busy.py", "gateway/base.py")})
        self.running = "0" * 40
        self.assertEqual(self.mismatches(self.root), ["running commit " + "0" * 40])
        self.running = None
        self.assertEqual(self.mismatches(self.root), ["running commit unknown"])

    def test_the_running_commit_is_hermes_own_code_identity(self):
        def identity(sha=None, error=None):
            def get_code_identity():
                if error:
                    raise error
                return {"sha": sha, "version": "0.22.2"}
            return {"hermes_cli": types.ModuleType("hermes_cli"),
                    "hermes_cli.version_info": NS(get_code_identity=get_code_identity)}

        for modules, expected in ((identity(COMMIT), COMMIT), (identity(None), None), (identity(""), None),
                                  (identity(error=RuntimeError("no git")), None)):
            with self.subTest(expected=expected), mock.patch.dict(sys.modules, modules):
                self.assertEqual(self.mod._running_commit(), expected)

    def test_the_library_version_is_the_one_this_process_loaded(self):
        # A venv sync replaces the files under a running gateway, not the module it imported.
        with mock.patch.dict(sys.modules, {"telegram": NS(__version__="99.1")}):
            self.assertEqual(self.mod._installed_version(PTB), "99.1")
        with mock.patch.dict(sys.modules):
            sys.modules.pop("telegram", None)
            try:
                from importlib import metadata
                expected = metadata.version(PTB)
            except Exception:
                expected = None
            self.assertEqual(self.mod._installed_version(PTB), expected, "not loaded: the installed one")

    def test_an_unreadable_pin_verifies_nothing(self):
        # Each case breaks exactly one field of a complete pin, so no check hides behind another.
        files = {"gateway/base.py": self.digest("gateway/base.py")}
        good = {"hermes_commit": COMMIT, "files": files, "packages": {PTB: "22.5"}}
        self.pin.write_text(json.dumps(good))
        self.assertEqual(self.mismatches(self.root), [], "the complete pin verifies")
        broken = [{"files": {}}, {"files": ["gateway/base.py"]}, {"files": None},
                  {"packages": {}}, {"packages": {PTB: ""}}, {"packages": [PTB]}, {"packages": None},
                  {"hermes_commit": None}, {"hermes_commit": "abc"}, {"hermes_commit": 123},
                  {"hermes_commit": "D" * 40}]
        texts = ["not json", json.dumps({}), json.dumps([])]
        texts += [json.dumps({**good, **change}) for change in broken]
        texts += [json.dumps({k: v for k, v in good.items() if k != key}) for key in good]
        for text in texts:
            with self.subTest(text=text):
                self.pin.write_text(text)
                self.assertEqual(self.mismatches(self.root), ["verified_hermes.json"])
        self.assertEqual(self.mismatches(self.root, pin=self.tmp / "missing.json"), ["verified_hermes.json"])

    def test_pins_never_reach_outside_the_hermes_checkout(self):
        outside = self.tmp / "outside.py"
        outside.write_text("x")
        digest = hashlib.sha256(b"x").hexdigest()
        self.write_pin({"../outside.py": digest, str(outside): digest})
        self.assertEqual(self.mismatches(self.root), sorted(["../outside.py", str(outside)]))

    def test_the_running_hermes_is_found_through_its_loaded_package(self):
        fake = types.ModuleType("hermes_cli")
        fake.__file__ = str(self.root / "hermes_cli/__init__.py")
        with mock.patch.dict(sys.modules, {"hermes_cli": fake}):
            self.assertEqual(self.mod.hermes_source_root(), self.root.resolve())

    def test_a_copied_message_is_found_only_as_whole_lines(self):
        mod = load_plugin()

        def holds(text, words):
            return mod._holds_lines(text, len(words), mod.fingerprint(words))

        self.assertTrue(holds("for four", "for four"))
        self.assertTrue(holds("book it\nfor four", "for four"))
        self.assertTrue(holds("for four\nbook it", "for four"))
        self.assertTrue(holds("a\n\nfor four\n\nb", "for four"), "captions are merged with a blank line")
        self.assertTrue(holds("a\nline one\nline two\nb", "line one\nline two"))
        self.assertFalse(holds("for four people", "for four"), "a line that only starts with the words")
        self.assertFalse(holds("say for four", "for four"), "a line that only ends with the words")
        self.assertFalse(holds("for five", "for four"), "same length, other words")
        self.assertFalse(holds("book it\nfor five", "for four"), "a whole last line of the same length, other words")
        self.assertFalse(holds(None, "for four"))
        self.assertFalse(holds("for", "for four"))

    def test_only_lines_that_start_at_or_after_a_point_count_when_one_is_given(self):
        mod = load_plugin()
        yes = mod.fingerprint("yes")
        before = "delete the old backups?\nyes"
        grown = before + "\nyes"
        self.assertTrue(mod._holds_lines(before, 3, yes))
        self.assertFalse(mod._holds_lines(before, 3, yes, len(before)), "that line was there before the point")
        self.assertTrue(mod._holds_lines(grown, 3, yes, len(before)), "appended after the point")
        self.assertFalse(mod._holds_lines(grown, 3, yes, len(before) + 2), "a line that starts before the point")
        self.assertTrue(mod._holds_lines("abc\nyes", 3, yes, 4), "a line that starts exactly at the point")
        self.assertTrue(mod._holds_lines("yes", 3, yes, 0), "an empty event that got the text")
        self.assertFalse(mod._holds_lines("yes", 3, yes, 9), "a point past the end")
        self.assertFalse(mod._holds_lines("noyes", 3, yes, 2), "a point inside a line")

    def test_an_in_place_replacement_moves_only_the_replaced_events_mark(self):
        mod = load_plugin()

        class Waiting:          # a pending event: weakly referenced, like Hermes' MessageEvent
            def __init__(self, text):
                self.text = text

        slot, fifo = Waiting("first wording"), Waiting("queued behind it")
        generation = mod.Generation(mod.shared_state("unit-rebase-" + str(id(self))), mod.ReplacementPolicy("88"),
                                    Gate())
        generation.cache.put(88, 2001, mod.Seen("later", mod.fingerprint("later"), marks=(
            (mod.weakref.ref(slot), len(slot.text)), (mod.weakref.ref(fifo), len(fifo.text)))))
        slot.text = "second"            # the plugin put the corrected text in place
        generation._rebase(slot)
        marks = [(ref(), size) for ref, size in generation.cache.get(88, 2001).marks]
        self.assertEqual(marks, [(slot, len("second")), (fifo, len("queued behind it"))])

    def test_the_committed_pin_covers_what_pin_hermes_pins(self):
        pin = load_pin_script()
        committed = json.loads((ROOT / "verified_hermes.json").read_text())
        self.assertEqual(sorted(committed["files"]), sorted(pin.RELIED_ON))
        for digest in committed["files"].values():
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
        self.assertEqual(sorted(committed["packages"]), sorted(pin.PACKAGES))
        self.assertEqual(tuple(pin.PACKAGES), tuple(self.mod.PINNED_PACKAGES))
        self.assertRegex(committed["packages"][PTB], r"^[0-9][0-9A-Za-z.+-]*$")
        self.assertRegex(committed["hermes_commit"], r"^[0-9a-f]{40}$")

    def test_a_pending_batch_carries_a_message_only_when_it_ends_with_its_text(self):
        mod = load_plugin()
        msg, other = object(), object()
        adapter = NS(_pending_text_batches={})
        self.assertEqual(mod._batch_of(adapter, "k", msg, "hello"), (None, False), "no batch")
        opened = NS(text="hello", raw_message=msg)
        adapter._pending_text_batches["k"] = opened
        self.assertEqual(mod._batch_of(adapter, "k", msg, "hello"), (opened, True), "it opened the batch")
        appended = NS(text="first\nhello", raw_message=other)
        adapter._pending_text_batches["k"] = appended
        self.assertEqual(mod._batch_of(adapter, "k", msg, "hello"), (appended, False), "its last line")
        for text in ("first", "first\nhello there", "firsthello", "hello\nfirst", ""):
            with self.subTest(text=text):
                adapter._pending_text_batches["k"] = NS(text=text, raw_message=other)
                self.assertEqual(mod._batch_of(adapter, "k", msg, "hello"), (None, False))


class PinScriptTests(unittest.TestCase):
    """scripts/pin_hermes.py pins the Hermes the suites ran against, and only that one."""

    def setUp(self):
        self.pin = load_pin_script()
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        self.repo = self.tmp / "hermes"
        for rel in self.pin.RELIED_ON:
            (self.repo / rel).parent.mkdir(parents=True, exist_ok=True)
            (self.repo / rel).write_text(f"{rel}\n")
        self.out = self.tmp / "verified_hermes.json"
        self.versions = {PTB: "22.5"}.get
        self.head = lambda repo: COMMIT
        self.commit_all(self.repo)       # a clean checkout: every relied-on file as committed

    @staticmethod
    def git(repo, *args):
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@localhost",
                        "-c", "commit.gpgsign=false", *args], check=True, capture_output=True, text=True)

    def commit_all(self, repo):
        self.git(repo, "init", "-q")
        self.git(repo, "add", "-A")
        self.git(repo, "commit", "-q", "-m", "hermes")

    def test_uncommitted_edits_to_relied_on_files_are_found(self):
        def edit(repo):
            (repo / "gateway/run_busy.py").write_text("edited by hand\n")

        def stage(repo):
            edit(repo)
            self.git(repo, "add", "gateway/run_busy.py")

        def untrack(repo):
            self.git(repo, "rm", "-q", "--cached", "gateway/run_busy.py")
            self.git(repo, "commit", "-q", "-m", "untracked")

        def ignore(repo):
            untrack(repo)
            (repo / ".gitignore").write_text("gateway/run_busy.py\n")

        def remove(repo):
            (repo / "gateway/run_busy.py").unlink()

        self.assertEqual(self.pin.uncommitted(self.repo), [], "a clean checkout")
        for name, change in (("unstaged", edit), ("staged", stage), ("untracked", untrack),
                             ("ignored", ignore), ("missing", remove)):
            with self.subTest(name):
                repo = self.tmp / name
                shutil.copytree(self.repo, repo)
                change(repo)
                self.assertEqual(self.pin.uncommitted(repo), ["gateway/run_busy.py"])
        plain = self.tmp / "no-git"
        shutil.copytree(self.repo, plain, ignore=shutil.ignore_patterns(".git"))
        self.assertTrue(self.pin.uncommitted(plain), "git can't tell: nothing counts as committed")

    def test_uncommitted_edits_are_not_pinned_and_the_suites_never_run(self):
        (self.repo / "gateway/run_busy.py").write_text("edited by hand\n")
        ran = []
        with contextlib.redirect_stdout(io.StringIO()) as said:
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: ran.append(1) or (True, "OK"),
                                             version_of=self.versions, commit_of=self.head), 0)
        self.assertFalse(self.out.exists())
        self.assertEqual(ran, [])
        self.assertIn("gateway/run_busy.py", said.getvalue())
        self.assertIn("uncommitted", said.getvalue())

    def test_uncommitted_edits_made_while_the_suites_run_are_not_pinned(self):
        # Only the index can differ when HEAD and every file's content stayed the same (a file untracked
        # while the suites ran): the check runs again afterwards.
        def suites():
            self.git(self.repo, "rm", "-q", "--cached", "gateway/run_busy.py")
            return True, "OK"

        with contextlib.redirect_stdout(io.StringIO()) as said:
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=suites, version_of=self.versions,
                                             commit_of=self.head), 0)
        self.assertFalse(self.out.exists())
        self.assertIn("gateway/run_busy.py", said.getvalue())
        self.assertIn("uncommitted", said.getvalue())

    def test_gateway_run_is_relied_on(self):
        self.assertIn("gateway/run.py", self.pin.RELIED_ON)

    def test_the_adapter_wiring_is_relied_on(self):
        # gateway/run_adapters.py attaches adapter.gateway_runner (the FIFO), the busy handler and the
        # topic recovery that _waiting and _waiting_marks rely on.
        self.assertIn("gateway/run_adapters.py", self.pin.RELIED_ON)

    def test_hermes_code_identity_is_relied_on(self):
        self.assertIn("hermes_cli/version_info.py", self.pin.RELIED_ON)

    def test_a_checkout_without_a_commit_is_not_pinned(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: (True, "OK"),
                                             version_of=self.versions, commit_of=lambda repo: None), 0)
        self.assertFalse(self.out.exists())

    def test_a_commit_that_moves_while_the_suites_run_is_not_pinned(self):
        heads = iter((COMMIT, "e" * 40))
        with contextlib.redirect_stdout(io.StringIO()) as said:
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: (True, "OK"),
                                             version_of=self.versions, commit_of=lambda repo: next(heads)), 0)
        self.assertFalse(self.out.exists())
        self.assertIn("HEAD", said.getvalue())

    def test_no_other_checkout_can_be_pinned(self):
        # The suites always run ~/.hermes/hermes-agent: pinning another tree would verify untested code.
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as stop:
                self.pin.main(["--root", str(self.repo)])
        self.assertEqual(stop.exception.code, 2)

    def test_it_pins_the_files_and_library_the_suites_ran_with(self):
        self.assertEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: (True, "OK"),
                                      version_of=self.versions, commit_of=self.head), 0)
        written = json.loads(self.out.read_text())
        self.assertEqual(written["hermes_commit"], COMMIT)
        self.assertEqual(sorted(written["files"]), sorted(self.pin.RELIED_ON))
        self.assertEqual(written["files"]["gateway/run.py"],
                         hashlib.sha256(b"gateway/run.py\n").hexdigest())
        self.assertEqual(written["packages"], {PTB: "22.5"})

    def test_files_that_change_while_the_suites_run_are_not_pinned(self):
        def suites():
            (self.repo / "gateway/run_busy.py").write_text("changed under the suites\n")
            return True, "OK"

        with contextlib.redirect_stdout(io.StringIO()) as said:
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=suites, version_of=self.versions,
                                             commit_of=self.head), 0)
        self.assertFalse(self.out.exists())
        self.assertIn("gateway/run_busy.py", said.getvalue())
        self.assertIn("changed while the suites ran", said.getvalue(), "the digests, not only the git check")

    def test_failing_suites_pin_nothing(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: (False, "FAILED"),
                                             version_of=self.versions, commit_of=self.head), 0)
        self.assertFalse(self.out.exists())

    def test_without_the_telegram_library_nothing_is_pinned(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertNotEqual(self.pin.pin(self.out, self.repo, run_suites=lambda: (True, "OK"),
                                             version_of={}.get, commit_of=self.head), 0)
        self.assertFalse(self.out.exists())


class PolicyTailTests(unittest.TestCase):
    def test_no_event_is_noop(self):
        self.assertIsNone(load_plugin().ReplacementPolicy("88").on_message())

    def test_bad_owner_disables_policy(self):
        for owner in (None, True, "", "not-an-id", "8" * 1000):
            with self.subTest(owner=owner):
                self.assertIsNone(load_plugin().ReplacementPolicy(owner).on_message(event=event()))


if __name__ == "__main__":
    unittest.main()
