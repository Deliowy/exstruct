from ..util import _util
from ._base_parser import BaseParser


class WebParser(BaseParser):
    """Parser for data-sources that provide data via web-pages"""

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        super().__init__(source, response_type, **kwargs)

    def parse(self):
        return super().parse()
