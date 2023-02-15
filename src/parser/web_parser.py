from ..util import util
from .base_parser import BaseParser

logger = util.getLogger("exstruct.parser.web_parser")

class WebParser(BaseParser):
    """Parser for data-sources that provide data via web-pages"""

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        super().__init__(source, response_type, **kwargs)

    def parse(self):
        return super().parse()
