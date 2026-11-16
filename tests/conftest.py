import importlib
import logging
import sys
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="session")
def rpc():
    # Only Kodi's platform boundary is substituted, not RPC, threads or sockets.
    with pytest.MonkeyPatch.context() as patch:
        patch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "resources" / "site-packages"))
        logger = ModuleType("elementum.logger")
        logger.log = logging.getLogger("bjsonrpc.tests")
        patch.setitem(sys.modules, "elementum.logger", logger)
        patch.setitem(sys.modules, "xbmc", ModuleType("xbmc"))
        yield importlib.import_module("bjsonrpc")
