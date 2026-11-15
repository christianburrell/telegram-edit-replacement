#!/usr/bin/env python3
"""Record the Hermes source this plugin is verified on: plugin/verified_hermes.json.

The plugin's delivery rules rest on Hermes internals (text batching, the busy queues and their drain,
plugin reloads) and on python-telegram-bot, which any Hermes update or venv sync can change, so it
stays inert on a Hermes whose relied-on files or Telegram library differ from this pin. After a Hermes
update: run this (it runs both test suites first and pins only when they pass), then
./install.sh <owner id> to put the new pin in place.

    python3 scripts/hermes_python.py -B scripts/pin_hermes.py

It pins ~/.hermes/hermes-agent (REPO), the checkout the gateway runs and the only one the suites test:
scripts/offline_pipeline.py imports REPO from here. There is deliberately no option to pin another
tree. Run it with the Hermes interpreter, or the pipeline tests (real PTB dispatch) are skipped and
nothing is pinned. The suites never read the committed pin: tests/test_hermes.py pins its throwaway copy.
"""
import argparse
import hashlib
from importlib import metadata
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
PIN = ROOT / "plugin" / "verified_hermes.json"
REPO = Path.home() / ".hermes/hermes-agent"          # the checkout the gateway runs (gateway.pid argv)
# Every Hermes file whose behaviour the plugin's rules and tests depend on.
RELIED_ON = (
    "gateway/platforms/base.py",                     # batching, busy routing, debounce, merges, holds
    "gateway/platforms/event.py",                    # MessageEvent, gateway control
    "gateway/run.py",                                # GatewayRunner: which busy, inbound and drain code runs
    "gateway/run_adapters.py",                       # adapter.gateway_runner, busy handler, topic recovery
    "gateway/run_busy.py",                           # next-turn slot, FIFO, steer and queue modes
    "gateway/run_inbound.py",                        # the runner's busy merges and prompt intercepts
    "gateway/run_plugin_rewire.py",                  # re-wiring after a plugin load
    "gateway/run_turn.py",                           # the drain that takes the next queued input
    "hermes_cli/plugins.py",                         # plugin context, forced reloads
    "hermes_cli/plugins_discovery.py",               # the enabled rule
    "hermes_cli/plugins_loader.py",                  # module loading and eviction
    "hermes_cli/version_info.py",                    # the commit a running process started from
    "plugins/platforms/telegram/adapter.py",         # handlers, events, flush, teardown
    "plugins/platforms/telegram/telegram_context.py",  # the bot's own @handle dropped from the text
    "plugins/platforms/telegram/telegram_entities.py",  # hidden links inlined
    "plugins/platforms/telegram/telegram_held_inbound.py",  # holds during teardown
    "plugins/platforms/telegram/update_admission.py",  # per-chat update order
)
# Libraries pinned with it (the plugin's PINNED_PACKAGES): PTB's handler groups and filters.
PACKAGES = ("python-telegram-bot",)


def installed_version(name):
    try:
        return metadata.version(name)
    except Exception:
        return None


def digests(repo):
    return {rel: hashlib.sha256((Path(repo) / rel).read_bytes()).hexdigest() for rel in RELIED_ON}


def versions(version_of):
    return {name: version_of(name) for name in PACKAGES}


def commit_of(repo):
    try:
        return subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True,
                              text=True, timeout=30).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def uncommitted(repo):
    """The relied-on files that differ from HEAD (staged or not), are missing, untracked or ignored:
    a pin of them would match a hand edit the running gateway may never have imported. Every relied-on
    file when git can't tell."""
    try:
        status = subprocess.run(["git", "--no-optional-locks", "-C", str(repo), "status", "--porcelain",
                                 "--untracked-files=all", "--ignored", "--", *RELIED_ON],
                                capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return list(RELIED_ON)
    if status.returncode != 0:
        return list(RELIED_ON)
    return sorted({line[3:] for line in status.stdout.splitlines() if line[3:]})


def write_record(path, commit, files, packages):
    record = {"hermes_commit": commit, "files": files, "packages": packages}
    Path(path).write_text(json.dumps(record, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def write_pin(path, repo, version_of=installed_version):
    """A pin of `repo` as it is now, with this interpreter's libraries (tests/test_hermes.py)."""
    write_record(path, commit_of(repo), digests(repo), versions(version_of))


def run_suites():
    """Both suites, run by this interpreter: (passed, the runner's last line)."""
    suite = subprocess.run([sys.executable, "-B", "-W", "error::ResourceWarning", "-m", "unittest", "discover",
                            "-s", "tests"], cwd=ROOT, capture_output=True, text=True)
    last = (suite.stderr.strip().splitlines() or [""])[-1]
    if suite.returncode != 0 or "skipped" in last:
        print(suite.stderr[-4000:])
        return False, last
    return True, last


def pin(path=PIN, repo=REPO, run_suites=run_suites, version_of=installed_version, commit_of=commit_of,
        uncommitted=uncommitted):
    """Pin `repo` in `path` once the suites passed, with exactly the commit, files and libraries they
    ran against: one that changed while they ran is not pinned, nor a relied-on file that differs from
    the commit (the plugin compares the commit with the one a running gateway started from)."""
    missing = [rel for rel in RELIED_ON if not (Path(repo) / rel).is_file()]
    if missing:
        print(f"not a Hermes checkout, or files moved: {missing}")
        return 2
    dirty = uncommitted(repo)
    if dirty:
        print(f"not pinned: uncommitted changes to relied-on files: {', '.join(dirty)}; commit or revert them")
        return 2
    before_commit, before_files, before_packages = commit_of(repo), digests(repo), versions(version_of)
    if not before_commit:
        print(f"not pinned: no git commit for {repo}")
        return 2
    absent = [name for name, version in before_packages.items() if not version]
    if absent:
        print(f"not pinned: {', '.join(absent)} is not installed for this interpreter (use the Hermes interpreter)")
        return 2
    passed, _last = run_suites()
    if not passed:
        print("not pinned: the suites must pass, with the pipeline tests run (use the Hermes interpreter)")
        return 1
    after_commit, after_files, after_packages = commit_of(repo), digests(repo), versions(version_of)
    changed = ["HEAD"] if after_commit != before_commit else []
    changed += sorted(rel for rel in RELIED_ON if before_files[rel] != after_files[rel])
    changed += sorted(name for name in PACKAGES if before_packages[name] != after_packages[name])
    if changed:
        print(f"not pinned: changed while the suites ran: {', '.join(changed)}; run it again")
        return 1
    dirty = uncommitted(repo)
    if dirty:
        print(f"not pinned: uncommitted changes to relied-on files: {', '.join(dirty)}; commit or revert them")
        return 1
    write_record(path, after_commit, after_files, after_packages)
    return 0


def main(argv=None):
    argparse.ArgumentParser(description="Pin ~/.hermes/hermes-agent after both suites pass.").parse_args(argv)
    code = pin()
    if code == 0:
        record = json.loads(PIN.read_text(encoding="utf-8"))
        print(f"pinned {len(record['files'])} files of Hermes {record['hermes_commit']} and "
              f"{', '.join(f'{name} {version}' for name, version in sorted(record['packages'].items()))} in {PIN}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
