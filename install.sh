#!/usr/bin/env bash
# Install or update telegram-edit-replacement in $HERMES_HOME (default ~/.hermes); no restart needed.
# `hermes plugins enable` asks the running gateway to reload its plugins only when the selection
# changes, so the plugin is always disabled and then enabled, with the gateway reloading its plugins in
# between (below); for those seconds the plugin reads itself disabled and edits pass through untagged.
# It then waits for the running gateway's load line for this copy: "loaded (pid <gateway pid>, active)".
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
HOME_DIR="${HERMES_HOME:-$HOME/.hermes}"
KEY=telegram-edit-replacement
OWNER="${1:?usage: install.sh <owner_telegram_user_id>}"
[[ "$OWNER" =~ ^[1-9][0-9]{0,19}$ ]] || { printf 'owner must be a Telegram user id: digits, no leading zero\n' >&2; exit 2; }
DEST="$HOME_DIR/plugins/$KEY"
mkdir -p "$DEST"
for f in __init__.py plugin.yaml verified_hermes.json; do
    install -m 0644 "$HERE/plugin/$f" "$DEST/$f"
done
hermes config set "plugins.entries.$KEY.settings.owner_user_id" "$OWNER"
# Only load lines written from here on count: any reload after this point loads these files with this
# owner setting. Hermes rotates agent.log by renaming it (agent.log.1, .2, ...), so each log file is
# remembered by inode, size and first bytes (an inode freed by a rotation can come back as a new file).
START="$(python3 - "$HOME_DIR/logs" <<'PY'
import json, os, re, sys
marks = {}
try:
    names = os.listdir(sys.argv[1])
except OSError:
    names = []
for name in names:
    if re.fullmatch(r"agent\.log(\.[0-9]+)?", name):
        try:
            with open(os.path.join(sys.argv[1], name), "rb") as fh:
                marks[str(os.fstat(fh.fileno()).st_ino)] = [os.fstat(fh.fileno()).st_size, fh.read(256).hex()]
        except OSError:
            pass
print(json.dumps(marks))
PY
)"
# `hermes plugins disable` sends the running gateway no reload, so the enable's reload would find this plugin
# still loaded, and a gateway wires Telegram handler factories only for the plugins a reload adds
# (hermes_cli/plugins_loader.py:229-237): one this version adds would wait for the next restart. So the gateway
# reloads its plugins in between, while this one is disabled: scripts/reload_gateway_plugins.py, run with Hermes'
# own Python ($HERMES_PYTHON, else the one venv that $HOME_DIR/installs/*/facts.json names). Best effort: without
# it the update still loads, and only such new handlers wait for the restart.
hermes_python() {
    if [ -n "${HERMES_PYTHON:-}" ]; then printf '%s\n' "$HERMES_PYTHON"; return 0; fi
    python3 - "$HOME_DIR/installs" <<'PY'
import json, sys
from pathlib import Path
found = set()
for facts in Path(sys.argv[1]).glob("*/facts.json"):
    try:
        python = Path(json.loads(facts.read_text())["packages"]["venv"]["environment"]) / "bin" / "python"
    except (OSError, KeyError, ValueError, TypeError):
        continue
    if python.is_file():
        found.add(str(python))
if len(found) == 1:
    print(found.pop())
PY
}
reload_while_disabled() {
    local python out why
    python="$(hermes_python)"
    if [ -z "$python" ]; then
        why="no single Hermes Python in $HOME_DIR/installs; set HERMES_PYTHON to it"
    elif out="$("$python" -I -B "$HERE/scripts/reload_gateway_plugins.py" "$HOME_DIR" 2>&1)"; then
        return 0
    else
        why="$(printf '%s\n' "$out" | tail -n 1)"
    fi
    printf 'note: the running gateway was not asked to reload its plugins (%s): Telegram handlers new in this version are wired at its next restart (README: Install)\n' "$why" >&2
}
trap 'printf "install.sh: stopped; %s may be disabled now. Run: hermes plugins enable %s\n" "$KEY" "$KEY" >&2' ERR
hermes plugins disable "$KEY"
reload_while_disabled
hermes plugins enable "$KEY"
trap - ERR
enabled_json="$(hermes config get plugins.enabled --json 2>/dev/null || true)"
disabled_json="$(hermes config get plugins.disabled --json 2>/dev/null || true)"
if ! python3 -c 'import json, sys
enabled, disabled = (json.loads(text or "null") for text in sys.argv[1:3])
listed = lambda value: isinstance(value, list) and sys.argv[3] in value
sys.exit(0 if listed(enabled) and not listed(disabled) else 1)' "$enabled_json" "$disabled_json" "$KEY" 2>/dev/null; then
    printf 'install.sh: %s is not enabled after `hermes plugins enable`; check `hermes plugins list`\n' "$KEY" >&2
    exit 1
fi
python3 - "$HOME_DIR" "$START" "${EDIT_INSTALL_WAIT:-60}" <<'PY'
import json, os, re, sys, time

home, marks, wait = sys.argv[1], json.loads(sys.argv[2]), float(sys.argv[3])
log_dir = os.path.join(home, "logs")
pid_file = os.path.join(home, "gateway.pid")


def started(pid):
    """The process's start time as gateway/status.py records it: field 22 of /proc/<pid>/stat."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            return int(fh.read().split()[21])
    except (OSError, IndexError, ValueError):
        return None


try:
    with open(pid_file, encoding="utf-8") as fh:
        record = json.load(fh)
    pid = int(record["pid"])
except FileNotFoundError:
    print("installed; no gateway runs from %s (no gateway.pid): one started later loads the plugin. A gateway "
          "started from another Hermes home that serves this profile (multiplexed profiles) keeps its "
          "gateway.pid there: after its next plugin reload, look for 'telegram-edit-replacement loaded "
          "(pid <its pid>, active)' in that home's logs/agent.log" % home)
    sys.exit(0)
except (OSError, ValueError, KeyError, TypeError):
    print("installed; %s names no gateway: one started later loads the plugin" % pid_file)
    sys.exit(0)
try:
    os.kill(pid, 0)
except OSError:
    print("installed; the gateway in %s is not running: it loads the plugin when it starts" % pid_file)
    sys.exit(0)
recorded, now = record.get("start_time"), started(pid)
if isinstance(recorded, int) and now is not None and now != recorded:
    print("installed; the gateway in %s is not running (its pid now belongs to another process): it loads "
          "the plugin when it starts" % pid_file)
    sys.exit(0)


def order(name):
    match = re.fullmatch(r"agent\.log(?:\.([0-9]+))?", name)
    return -int(match.group(1)) if match and match.group(1) else 0


def new_lines():
    """Lines written since START, oldest file first: a log file that existed then is read from where it
    ended (from its start if it shrank), one created since is read whole."""
    try:
        names = sorted((n for n in os.listdir(log_dir) if re.fullmatch(r"agent\.log(\.[0-9]+)?", n)), key=order)
    except OSError:
        return []
    lines = []
    for name in names:
        try:
            with open(os.path.join(log_dir, name), "rb") as fh:
                st = os.fstat(fh.fileno())
                size, head = marks.get(str(st.st_ino), (None, ""))
                same = size is not None and st.st_size >= size and fh.read(len(head) // 2).hex() == head
                fh.seek(size if same else 0)
                lines.extend(fh.read().decode("utf-8", "replace").splitlines())
        except OSError:
            continue
    return lines


line = re.compile(r"telegram-edit-replacement loaded \(pid %d, (.*)\)$" % pid)
deadline = time.monotonic() + wait
while True:
    found = [m.group(1) for m in map(line.search, new_lines()) if m]
    if found:
        if found[-1] == "active":
            print("installed; the running gateway (pid %d) loaded it: active" % pid)
            sys.exit(0)
        print("install.sh: the gateway (pid %d) loaded it, but: %s" % (pid, found[-1]), file=sys.stderr)
        sys.exit(1)
    if time.monotonic() > deadline:
        print("install.sh: the running gateway (pid %d) did not report loading this copy; look for "
              "telegram-edit-replacement in %s, or load it with: hermes gateway restart"
              % (pid, os.path.join(log_dir, "agent.log")), file=sys.stderr)
        sys.exit(1)
    time.sleep(0.5)
PY
printf 'installed: %s\n' "$DEST"
