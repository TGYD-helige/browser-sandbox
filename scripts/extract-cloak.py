"""Extract the digest-pinned upstream browser and its default arguments at build time."""

import json
from pathlib import Path
import shutil

from cloakbrowser import ensure_binary
from cloakbrowser.browser import build_args

binary = Path(ensure_binary())
if binary.name != "chrome":
    raise RuntimeError("Expected Chrome executable for AppArmor policy attachment")
shutil.copytree(binary.parent, "/cloak")
Path("/cloak/args.json").write_text(json.dumps(build_args(True, [])))
