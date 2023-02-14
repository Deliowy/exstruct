import typing
from pathlib import Path

from ..util import util
from .base_exstruct import BaseExStruct

logger = util.getLogger("exstruct.exstruct.csv_exstruct")


class CSVExStruct(BaseExStruct):
    """Extractor of data structure from CSV file"""

    def __init__(
        self,
        data_type_mapping: dict,
        external_id_collection: tuple,
        mapping_delimiter: str = " -> ",
        source_path: Path = None,
        result_path: Path = None,
    ) -> None:
        super().__init__(
            data_type_mapping,
            external_id_collection,
            mapping_delimiter,
            source_path,
            result_path,
        )

    def make_structure(
        self,
        source_content: str,
        ignored_fields: typing.Iterable[str] = None,
        ignore_levels: int = 0,
    ):
        return super().make_structure(source_content, ignored_fields, ignore_levels)
