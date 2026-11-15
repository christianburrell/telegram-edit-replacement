"""install.sh against a stub `hermes` on PATH and a throwaway HERMES_HOME: never the real CLI."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
KEY = "telegram-edit-replacement"
# The stub keeps plugins.enabled / plugins.disabled as one key per line in $STUB_STATE/{enabled,disabled};
# an enable stands in for the running gateway's reload by writing the plugin's load line to agent.log.
STUB = r"""#!/usr/bin/env bash
printf '%s\n' "$*" >> "$STUB_LOG"
on() { grep -qxF "$2" "$STUB_STATE/$1" 2>/dev/null; }
add() { on "$1" "$2" || printf '%s\n' "$2" >> "$STUB_STATE/$1"; }
drop() { if [ -f "$STUB_STATE/$1" ]; then grep -vxF "$2" "$STUB_STATE/$1" > "$STUB_STATE/$1.tmp" || true; mv "$STUB_STATE/$1.tmp" "$STUB_STATE/$1"; fi; }
rotate() {   # as Hermes' RotatingFileHandler: agent.log.1 -> agent.log.2, agent.log -> agent.log.1, a new agent.log
  if [ -f "$HERMES_HOME/logs/agent.log.1" ]; then mv "$HERMES_HOME/logs/agent.log.1" "$HERMES_HOME/logs/agent.log.2"; fi
  if [ -f "$HERMES_HOME/logs/agent.log" ]; then mv "$HERMES_HOME/logs/agent.log" "$HERMES_HOME/logs/agent.log.1"; fi
  : > "$HERMES_HOME/logs/agent.log"
}
case "$1 $2" in
  "plugins enable")
    if [ "${STUB_FAIL:-}" = enable ]; then echo "enable failed" >&2; exit 1; fi
    if [ "${STUB_FAIL:-}" = noop ]; then exit 0; fi
    if [ "${STUB_FAIL:-}" = half ]; then add enabled "$3"; exit 0; fi
    drop disabled "$3"; add enabled "$3"
    mkdir -p "$HERMES_HOME/logs"
    if [ -n "${STUB_ROTATE:-}" ]; then rotate; fi
    if [ "${STUB_TRUNCATE:-}" = fresh ]; then : > "$HERMES_HOME/logs/agent.log"; fi    # same file, new content
    if [ "${STUB_TRUNCATE:-}" = keephead ]; then                                       # same file, shorter
      head -c 256 "$HERMES_HOME/logs/agent.log" > "$STUB_STATE/head"
      cat "$STUB_STATE/head" > "$HERMES_HOME/logs/agent.log"
    fi
    if [ -n "${STUB_LOAD_STATUS:-}" ]; then
      ( sleep "${STUB_LOAD_DELAY:-0}"
        if [ -n "${STUB_EXTRA_STATUS:-}" ]; then     # an earlier reload, logged first
          printf '2000-01-05 01:59:59,000 INFO hermes.plugins.telegram_edit_replacement: telegram-edit-replacement loaded (pid %s, %s)\n' \
            "$STUB_LOAD_PID" "$STUB_EXTRA_STATUS" >> "$HERMES_HOME/logs/agent.log"
        fi
        printf '2000-01-05 02:00:00,000 INFO hermes.plugins.telegram_edit_replacement: telegram-edit-replacement loaded (pid %s, %s)\n' \
          "$STUB_LOAD_PID" "$STUB_LOAD_STATUS" >> "$HERMES_HOME/logs/agent.log"
        if [ -n "${STUB_FILLER:-}" ]; then
          head -c "$STUB_FILLER" /dev/zero | tr '\0' x >> "$HERMES_HOME/logs/agent.log"; echo >> "$HERMES_HOME/logs/agent.log"
        fi
        if [ -n "${STUB_ROTATE_AFTER:-}" ]; then rotate; fi ) &
    fi ;;
  "plugins disable")
    drop enabled "$3"; add disabled "$3" ;;
  "config set")
    if [ -n "${STUB_CONFIG_LINE:-}" ]; then      # another process reloaded the gateway meanwhile
      printf '2000-01-05 01:59:00,000 INFO hermes.plugins.telegram_edit_replacement: telegram-edit-replacement loaded (pid %s, %s)\n' \
        "$STUB_LOAD_PID" "$STUB_CONFIG_LINE" >> "$HERMES_HOME/logs/agent.log"
    fi ;;
  "config get")
    list="${3#plugins.}"
    if [ ! -s "$STUB_STATE/$list" ]; then echo "Config key not set: $3" >&2; exit 1; fi
    python3 -c 'import json, sys; print(json.dumps([l for l in open(sys.argv[1]).read().splitlines() if l]))' "$STUB_STATE/$list" ;;
esac
"""
# Stands in for Hermes' Python, which install.sh runs scripts/reload_gateway_plugins.py with: it logs the script,
# the Hermes home it was given and whether the plugin was disabled at that moment, then answers as told.
STUB_PY = r"""#!/usr/bin/env bash
script=""; for arg in "$@"; do case "$arg" in *.py) script="$(basename "$arg")" ;; esac; done
state=enabled; if grep -qxF telegram-edit-replacement "$STUB_STATE/disabled" 2>/dev/null; then state=disabled; fi
printf 'python %s %s %s\n' "$script" "${@: -1}" "$state" >> "$STUB_LOG"
case "${STUB_RELOAD:-reloaded}" in
  reloaded) echo "a plugin's warning on stdout"; echo "RELOAD reloaded" ;;
  none) echo "RELOAD no gateway" ;;
  fail) echo "Traceback (most recent call last): ImportError: no gateway.control_socket" >&2; exit 1 ;;
esac
"""


class InstallScriptTests(unittest.TestCase):
    def install(self, owner, enabled=(), fail="", gateway=None, status="active", earlier_line=False,
                load_pid=None, delay=0, wait=1, rotate=False, rotate_after=False, config_line=None,
                start_time=None, old_rotated_line=False, log_padding=0, truncate="", filler=0,
                extra_status="", python="env", installs=1, reload="reloaded"):
        """gateway: None (no gateway.pid), "running" (this test's own live pid) or "dead" (an exited
        process). status: what the gateway's reload logs after the enable, None for nothing.
        start_time: None (not recorded), "right" or "wrong" (the pid now runs another process).
        python: how install.sh finds Hermes' Python: "env" (HERMES_PYTHON), "facts" (`installs` venvs listed in
        $HERMES_HOME/installs/*/facts.json, as Hermes' installer writes them) or "none". reload: what the
        stand-in Python answers: "reloaded", "none" (no gateway answered) or "fail"."""
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))))
        bindir, state = tmp / "bin", tmp / "state"
        bindir.mkdir()
        state.mkdir()
        (tmp / "user").mkdir()
        hermes_home = tmp / "home"
        (hermes_home / "logs").mkdir(parents=True)
        pid = os.getpid()
        if gateway == "dead":
            gone = subprocess.Popen(["true"])
            gone.wait()
            pid = gone.pid
        if gateway is not None:
            record = {"pid": pid, "kind": "hermes-gateway"}
            if start_time is not None:     # as gateway/status.py records it: /proc/<pid>/stat field 22
                started = int(Path(f"/proc/{pid}/stat").read_text().split()[21])
                record["start_time"] = started if start_time == "right" else started + 12345
            (hermes_home / "gateway.pid").write_text(json.dumps(record))
        if old_rotated_line:   # a load logged before the last rotation, long before this install
            (hermes_home / "logs/agent.log.1").write_text(
                f"2000-01-03 09:00:00,000 INFO x: telegram-edit-replacement loaded (pid {pid}, active)\n")
        if log_padding:        # unrelated log lines from before this install
            (hermes_home / "logs/agent.log").write_text("".join(
                f"2000-01-04 08:00:{n:02d},000 INFO other.module: unrelated line {n:03d} {'.' * 20}\n"
                for n in range(log_padding)))
        if earlier_line:       # a load from before this install must not count
            (hermes_home / "logs/agent.log").write_text(
                f"2000-01-04 09:00:00,000 INFO x: telegram-edit-replacement loaded (pid {pid}, active)\n")
        if enabled:
            (state / "enabled").write_text("".join(f"{key}\n" for key in enabled))
        stub = bindir / "hermes"
        stub.write_text(STUB)
        stub.chmod(0o755)
        stub_py = tmp / "hermes-python"
        stub_py.write_text(STUB_PY)
        stub_py.chmod(0o755)
        if python == "facts":
            for n in range(installs):
                venv = tmp / f"env{n}/venv"
                (venv / "bin").mkdir(parents=True)
                (venv / "bin/python").write_text(STUB_PY)
                (venv / "bin/python").chmod(0o755)
                facts = hermes_home / f"installs/inst{n}/facts.json"
                facts.parent.mkdir(parents=True)
                facts.write_text(json.dumps({"packages": {"venv": {"environment": str(venv)}}}))
        log = tmp / "calls.log"
        env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "HERMES_HOME": str(hermes_home),
               "HOME": str(tmp / "user"),
               "STUB_LOG": str(log), "STUB_STATE": str(state), "STUB_FAIL": fail,
               "STUB_LOAD_PID": str(load_pid or pid), "STUB_LOAD_STATUS": status or "", "STUB_LOAD_DELAY": str(delay),
               "EDIT_INSTALL_WAIT": str(wait), "STUB_ROTATE": "1" if rotate else "",
               "STUB_ROTATE_AFTER": "1" if rotate_after else "", "STUB_CONFIG_LINE": config_line or "",
               "STUB_TRUNCATE": truncate, "STUB_FILLER": str(filler) if filler else "",
               "STUB_EXTRA_STATUS": extra_status, "STUB_RELOAD": reload}
        env.pop("HERMES_PYTHON", None)
        if python == "env":
            env["HERMES_PYTHON"] = str(stub_py)
        proc = subprocess.run(["bash", str(HERE / "install.sh"), owner], env=env, capture_output=True,
                              text=True, timeout=30)
        calls = log.read_text().splitlines() if log.exists() else []
        final = {name: (state / name).read_text().split() if (state / name).exists() else []
                 for name in ("enabled", "disabled")}
        return proc, calls, tmp / "home/plugins" / KEY, final

    def test_install_and_update_cycle_the_plugin_so_the_gateway_loads_this_code(self):
        # `hermes plugins enable` asks the gateway to reload only when the selection changes, so the
        # plugin is always disabled first: a first install and an update take the same path.
        for enabled in ((), ("other-plugin",), ("other-plugin", KEY)):
            with self.subTest(enabled=enabled):
                proc, calls, dest, final = self.install("1000000001", enabled)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(calls, [
                    f"config set plugins.entries.{KEY}.settings.owner_user_id 1000000001",
                    f"plugins disable {KEY}",
                    f"python reload_gateway_plugins.py {dest.parents[1]} disabled",
                    f"plugins enable {KEY}",
                    "config get plugins.enabled --json",
                    "config get plugins.disabled --json",
                ])
                self.assertIn(KEY, final["enabled"])
                self.assertNotIn(KEY, final["disabled"])
                for name in ("__init__.py", "plugin.yaml", "verified_hermes.json"):
                    self.assertEqual((dest / name).read_bytes(), (HERE / "plugin" / name).read_bytes())

    def test_the_gateway_reloads_its_plugins_while_this_one_is_disabled(self):
        # `hermes plugins disable` sends the gateway no reload, so without one in between, the enable's reload
        # finds the plugin still loaded and wires no Telegram handler factory this version adds (README, Wiring
        # that survives reloads). Hermes' Python comes from HERMES_PYTHON, else from the installer's facts.
        for python in ("env", "facts"):
            with self.subTest(python=python):
                proc, calls, dest, _final = self.install("1000000001", python=python)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(calls[1:4], [f"plugins disable {KEY}",
                                              f"python reload_gateway_plugins.py {dest.parents[1]} disabled",
                                              f"plugins enable {KEY}"])
                self.assertNotIn("note:", proc.stdout + proc.stderr)

    def test_without_one_hermes_python_the_install_goes_on_with_a_note(self):
        # No interpreter found, or two installs to choose from: the update still loads; only Telegram
        # handlers new in this version wait for the gateway's next restart.
        for python, installs in (("none", 1), ("facts", 2)):
            with self.subTest(python=python, installs=installs):
                proc, calls, _dest, final = self.install("1000000001", python=python, installs=installs)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual([call for call in calls if call.startswith("python")], [])
                self.assertEqual(calls[1:3], [f"plugins disable {KEY}", f"plugins enable {KEY}"])
                self.assertIn(KEY, final["enabled"])
                self.assertIn("note: the running gateway was not asked to reload its plugins", proc.stderr)
                self.assertIn("HERMES_PYTHON", proc.stderr)
                self.assertIn("next restart", proc.stderr)

    def test_a_reload_that_fails_is_only_a_note(self):
        proc, calls, _dest, final = self.install("1000000001", reload="fail")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(calls[3], f"plugins enable {KEY}")
        self.assertIn(KEY, final["enabled"])
        self.assertIn("note: the running gateway was not asked to reload its plugins", proc.stderr)
        self.assertIn("ImportError: no gateway.control_socket", proc.stderr, "the reason is shown")
        self.assertIn("next restart", proc.stderr)

    def test_no_gateway_to_reload_needs_no_note(self):
        proc, calls, _dest, _final = self.install("1000000001", reload="none")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(calls[3], f"plugins enable {KEY}")
        self.assertNotIn("note:", proc.stdout + proc.stderr)

    def test_a_failed_enable_says_how_to_recover(self):
        proc, _calls, _dest, final = self.install("1000000001", ("other-plugin", KEY), fail="enable")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn(KEY, final["disabled"], "the stub left it disabled, as a real failure could")
        self.assertIn(f"hermes plugins enable {KEY}", proc.stderr)

    def test_an_enable_that_changed_nothing_is_reported(self):
        proc, _calls, _dest, _final = self.install("1000000001", (), fail="noop")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("is not enabled", proc.stderr)

    def test_an_enable_that_left_it_on_the_deny_list_is_reported(self):
        # Hermes' rule: plugins.disabled wins over plugins.enabled.
        proc, _calls, _dest, final = self.install("1000000001", (), fail="half")
        self.assertIn(KEY, final["enabled"])
        self.assertIn(KEY, final["disabled"])
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("is not enabled", proc.stderr)

    def test_install_waits_until_the_running_gateway_loaded_this_copy(self):
        for delay in (0, 1):            # the gateway reloads in the background: the line can come later
            with self.subTest(delay=delay):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", delay=delay, wait=5)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("active", proc.stdout)

    def test_a_rotated_log_is_read_from_its_start(self):
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", earlier_line=True, rotate=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_a_log_rotated_right_after_the_load_line_still_counts(self):
        for earlier_line in (False, True):     # agent.log new since the start, or one that existed then
            with self.subTest(earlier_line=earlier_line):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", rotate_after=True,
                                                           earlier_line=earlier_line, wait=5)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("active", proc.stdout)

    def test_an_older_rotated_log_never_counts(self):
        for rotate in (False, True):
            with self.subTest(rotate=rotate):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", status=None,
                                                           old_rotated_line=True, earlier_line=True,
                                                           rotate=rotate)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("did not report loading", proc.stderr)

    def test_a_log_rewritten_in_place_is_read_from_its_start(self):
        # Same inode, other content: a truncate-and-rewrite, or an inode a rotation freed coming back.
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", log_padding=20,
                                                   truncate="fresh", filler=4000, wait=5)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("active", proc.stdout)

    def test_a_log_that_shrank_is_read_from_its_start(self):
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", log_padding=20,
                                                   truncate="keephead", wait=5)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("active", proc.stdout)

    def test_the_newest_load_line_counts(self):
        for extra, status, ok in (("inactive: no owner_user_id", "active", True),
                                  ("active", "inactive: no owner_user_id", False)):
            with self.subTest(extra=extra, status=status):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", status=status,
                                                           extra_status=extra, wait=5)
                self.assertEqual(proc.returncode == 0, ok, proc.stdout + proc.stderr)

    def test_a_load_line_from_before_the_enable_does_not_count(self):
        # A reload by another process while the files and the owner setting were written.
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", delay=1, wait=5,
                                                   config_line="inactive: no owner_user_id")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("active", proc.stdout)

    def test_a_reused_pid_in_gateway_pid_is_not_a_running_gateway(self):
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", status=None,
                                                   start_time="wrong")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("is not running", proc.stdout)
        self.assertIn("another process", proc.stdout)
        proc, _calls, _dest, _final = self.install("1000000001", gateway="running", start_time="right")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("loaded it: active", proc.stdout)

    def test_only_the_gateways_own_load_line_counts(self):
        other = subprocess.Popen(["sleep", "30"])         # a live process that is not the gateway
        try:
            proc, _calls, _dest, _final = self.install("1000000001", gateway="running", load_pid=other.pid)
        finally:
            other.kill()
            other.wait()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("did not report loading", proc.stderr)

    def test_a_gateway_that_did_not_load_this_copy_is_reported(self):
        for earlier_line in (False, True):
            with self.subTest(earlier_line=earlier_line):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", status=None,
                                                           earlier_line=earlier_line)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("did not report loading", proc.stderr)
                self.assertIn("hermes gateway restart", proc.stderr)

    def test_an_inactive_load_is_reported_with_its_reason(self):
        for status in ("inactive: Hermes differs from the version this plugin was verified on",
                       "active; restart the gateway to update its Telegram handlers"):
            with self.subTest(status=status):
                proc, _calls, _dest, _final = self.install("1000000001", gateway="running", status=status)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(status, proc.stderr)

    def test_without_a_running_gateway_there_is_nothing_to_wait_for(self):
        proc, _calls, _dest, _final = self.install("1000000001", gateway="dead", status=None)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("is not running", proc.stdout)

    def test_without_gateway_pid_it_says_where_else_to_look(self):
        # A multiplexed gateway serves this profile from another home and keeps gateway.pid there.
        proc, _calls, _dest, _final = self.install("1000000001", gateway=None, status=None)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("no gateway.pid", proc.stdout)
        self.assertIn("another Hermes home", proc.stdout)

    def test_owner_must_be_a_plain_telegram_user_id(self):
        for owner in ("088", "abc", "88 ", "-88", "1" * 21, ""):
            with self.subTest(owner=owner):
                proc, calls, dest, _final = self.install(owner)
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(calls, [])
                self.assertFalse(dest.exists())


if __name__ == "__main__":
    unittest.main()
