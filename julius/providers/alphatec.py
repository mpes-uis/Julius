"""Alphatec transparency portal provider.

Identical API pattern to Agape — same paginated JSON structure and URL format.
Only the YAML endpoints file differs (for future divergence).
"""

from pathlib import Path
from typing import ClassVar

from julius.providers.agape import Agape
from julius.providers.registry import register

_ENDPOINTS_FILE = Path(__file__).parent.parent.parent / "data" / "endpoints" / "alphatec.yaml"


@register
class Alphatec(Agape):
    name: ClassVar[str] = "alphatec"

    def _endpoints_file(self) -> Path:
        return _ENDPOINTS_FILE
