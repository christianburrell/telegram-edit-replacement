import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
PLUGIN = Path(os.environ.get("EDIT_PLUGIN_DIR", HERE / "plugin"))
PIPELINE_TESTS = 153

class HermesTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec("telegram"), "Hermes interpreter required")
    def test_real_dispatch_registration_and_busy_path(self):
        with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as tmp:
            home = Path(tmp) / "home"
            home.mkdir()
            shutil.copytree(PLUGIN, home / "plugins/telegram-edit-replacement")
            # The copy is verified on the Hermes these tests run, whatever the committed pin says:
            # the suites decide whether a new Hermes may be pinned (scripts/pin_hermes.py).
            spec = importlib.util.spec_from_file_location("pin_hermes_for_tests", HERE / "scripts/pin_hermes.py")
            assert spec is not None and spec.loader is not None
            pin = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(pin)
            pin.write_pin(home / "plugins/telegram-edit-replacement/verified_hermes.json", pin.REPO)
            (home / "config.yaml").write_text("plugins:\n  enabled: [telegram-edit-replacement]\n  entries:\n    telegram-edit-replacement:\n      settings:\n        owner_user_id: '88'\n")
            env = {k: v for k, v in os.environ.items() if not
                (k.startswith(("HERMES_SESSION_", "HERMES_PARENT_")) or
                 any(s in k for s in ("API_KEY", "BOT_TOKEN", "AUTH_TOKEN")))}
            env["HERMES_HOME"] = str(home)
            # Token-scoped gateway locks go here, never into the real ~/.local/state/hermes.
            env["HERMES_GATEWAY_LOCK_DIR"] = str(Path(tmp) / "locks")
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            # No launch preparation (update tail, dependency sync, relaunch) from hermes_bootstrap.
            env["HERMES_DISABLE_LAZY_INSTALLS"] = "1"
            p = subprocess.run([sys.executable, "-B", "-W", "error::ResourceWarning",
                str(HERE / "scripts/offline_pipeline.py")], cwd=HERE, env=env,
                capture_output=True, text=True, timeout=240)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn(f"Ran {PIPELINE_TESTS} tests", p.stderr)
