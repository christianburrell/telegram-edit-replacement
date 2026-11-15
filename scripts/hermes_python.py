#!/usr/bin/env python3
"""Run a command with the Python interpreter Hermes itself runs with, or print its path.

The integration tests import Hermes (gateway, hermes_cli, plugins) and python-telegram-bot, which live in
Hermes' own virtual environment, not in the system Python. This finds that interpreter without starting
Hermes: no launcher, no locks, no package updates.

    python3 scripts/hermes_python.py --path             # print the interpreter
    python3 scripts/hermes_python.py -m unittest ...     # run it with these arguments (and -B)

Which interpreter:
  1. $HERMES_PYTHON, when set (it must be an executable file);
  2. otherwise the one virtual environment named by $HERMES_HOME/installs/*/facts.json
     (packages.venv.environment), as Hermes' package manager records it; HERMES_HOME defaults to ~/.hermes.
It stops with a message and exit status 2 when it finds none, or more than one.
"""
import json
import os
from pathlib import Path
import sys


def hermes_home():
    return Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes").expanduser()


def locate():
    """The interpreter's path, or raise LookupError saying why there is none."""
    override = os.environ.get("HERMES_PYTHON")
    if override:
        python = Path(override).expanduser()
        if not (python.is_file() and os.access(python, os.X_OK)):
            raise LookupError(f"HERMES_PYTHON={override} is not an executable file")
        return python
    installs = hermes_home() / "installs"
    found = set()
    for facts in sorted(installs.glob("*/facts.json")):
        try:
            environment = json.loads(facts.read_text(encoding="utf-8"))["packages"]["venv"]["environment"]
            python = Path(environment) / "bin" / "python"
        except (OSError, KeyError, ValueError, TypeError):
            continue
        if python.is_file():
            found.add(python)
    if len(found) == 1:
        return found.pop()
    if not found:
        raise LookupError(f"no Hermes virtual environment is named in {installs}/*/facts.json")
    raise LookupError(f"more than one Hermes virtual environment in {installs}: "
                      + ", ".join(sorted(map(str, found))))


def main(argv):
    try:
        python = locate()
    except LookupError as exc:
        print(f"hermes_python.py: cannot find Hermes' Python: {exc}.\n"
              "Set HERMES_PYTHON to the interpreter Hermes runs with, or HERMES_HOME to your Hermes home.",
              file=sys.stderr)
        return 2
    if not argv or argv == ["--path"]:
        print(python)
        return 0
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.execv(str(python), [str(python), "-B", *argv])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
