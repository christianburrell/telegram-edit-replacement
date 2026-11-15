"""scripts/reload_gateway_plugins.py, run the way install.sh runs it (Hermes' Python, isolated mode), against
Hermes' real control socket server in a throwaway home: never the live gateway."""
import asyncio
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
SCRIPT = HERE / "scripts/reload_gateway_plugins.py"


@unittest.skipUnless(importlib.util.find_spec("gateway"), "Hermes interpreter required")
class ReloadScriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        self.user = self.tmp / "user"            # HOME: Hermes' default root is $HOME/.hermes
        self.user.mkdir()

    def run_script(self, home):
        env = {k: v for k, v in os.environ.items() if not k.startswith("HERMES_")}
        env.update(HOME=str(self.user), HERMES_HOME=str(home), PYTHONDONTWRITEBYTECODE="1",
                   HERMES_DISABLE_LAZY_INSTALLS="1")
        return subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), str(home)], env=env, capture_output=True,
                              text=True, timeout=60)

    def serve(self, socket_home, home):
        """Run the script while a Hermes control socket server for socket_home answers reload-plugins."""
        from gateway.control_socket import GatewayControlServer
        asked = []

        def reload_plugins(params):
            asked.append(params)
            return {"reloaded": True, "home": params.get("home")}

        async def scenario():
            server = GatewayControlServer(socket_home, verb_handlers={"reload-plugins": reload_plugins})
            self.assertTrue(await server.start())
            try:
                return await asyncio.to_thread(self.run_script, home)
            finally:
                await server.stop()

        return asyncio.run(scenario()), asked

    def test_with_no_gateway_it_says_so(self):
        home = self.tmp / "home"
        home.mkdir()
        p = self.run_script(home)
        self.assertEqual((p.returncode, p.stdout.splitlines()[-1:]), (0, ["RELOAD no gateway"]), p.stderr)

    def test_the_gateway_serving_the_home_reloads_its_plugins(self):
        home = self.tmp / "home"
        home.mkdir()
        p, asked = self.serve(home, home)
        self.assertEqual((p.returncode, p.stdout.splitlines()[-1:]), (0, ["RELOAD reloaded"]), p.stderr)
        self.assertEqual(asked, [{"home": str(home)}])

    def test_a_profile_served_from_its_root_asks_the_roots_gateway(self):
        # Multiplexed profiles: the gateway runs from the root and serves the profile's home, as
        # `hermes plugins enable` asks it (hermes_cli/plugins_activation.py activate_plugin_now).
        root = self.user / ".hermes"
        home = root / "profiles/work"
        home.mkdir(parents=True)
        p, asked = self.serve(root, home)
        self.assertEqual((p.returncode, p.stdout.splitlines()[-1:]), (0, ["RELOAD reloaded"]), p.stderr)
        self.assertEqual(asked, [{"home": str(home)}])


if __name__ == "__main__":
    unittest.main()
