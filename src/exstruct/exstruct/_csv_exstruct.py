import tempfile
import typing
from pathlib import Path

import pandas

from ..util import _util
from ._base_exstruct import BaseExStruct

logger = _util.getLogger("exstruct.exstruct.csv_exstruct")


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

    def read_source(self, source_path: Path = None, *args, **kwargs):
        if source_path:
            filepath = source_path
        else:
            filepath = self.source_path

        return pandas.read_csv(filepath_or_buffer=filepath, **kwargs).convert_dtypes()

    def make_structure(
        self,
        source_content: pandas.DataFrame,
        ignored_fields: typing.Iterable[str] = None,
        ignore_levels: int = 0,
        structure_name: str = None,
    ):
        columns = {}

        for column_name, column_type in source_content.dtypes.to_dict().items():
            collected_info_settings = self.parse_element({column_name: column_type})
            columns[column_name] = {"@collected_info": collected_info_settings}

        data_structure = {structure_name: columns}

        return data_structure

    def parse_element(self, element: dict, ignore_levels: int = 0):
        collected_info_settings = {}
        element_name, element_data = tuple(*element.items())

        collected_info_settings["annotation"] = ""
        collected_info_settings["aliases"] = [element_name]
        collected_info_settings["collected_info_type"] = "V"
        collected_info_settings["type"] = self.data_type_mapping[element_data.name]
        collected_info_settings["occurence"] = False
        collected_info_settings["external_id"] = (
            True if element_name.lower() in self.external_id_collection else False
        )
        collected_info_settings["path"] = ""
        collected_info_settings["mapping"] = ""
        return collected_info_settings
