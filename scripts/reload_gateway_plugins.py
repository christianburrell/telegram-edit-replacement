"""Ask the running gateway to reload its plugins now. install.sh runs this with Hermes' own Python, between
`hermes plugins disable` and `hermes plugins enable`: disable sends the gateway no reload, so without this the
enable's reload would find the plugin still loaded, and a gateway wires Telegram handler factories only for the
plugins a reload adds (hermes_cli/plugins_loader.py:229-237, nothing new = no event). A factory a new version adds
would then wait for the gateway's next restart.

Usage: HERMES_PYTHON -I -B scripts/reload_gateway_plugins.py HERMES_HOME
Prints one line, `RELOAD reloaded` or `RELOAD no gateway` (no gateway answered). An import error (another Hermes)
exits non-zero, and install.sh shows its last line as a note.
"""
import sys
from pathlib import Path

from gateway.control_socket import reload_gateway_plugins
from hermes_constants import get_default_hermes_root


def reload(home):
    """The gateway serving `home` reloads its plugins: its own socket, else (a profile a gateway serves from its
    root, multiplexed) the root's, as `hermes plugins enable` asks it (hermes_cli/plugins_activation.py)."""
    answer = reload_gateway_plugins(home)
    root = Path(get_default_hermes_root(home=home))
    if answer is None and root.resolve() != home.resolve():
        answer = reload_gateway_plugins(root, profile_home=home)
    return "reloaded" if answer and answer.get("reloaded") else "no gateway"


if __name__ == "__main__":
    print("RELOAD", reload(Path(sys.argv[1]).expanduser()))
