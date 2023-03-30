import abc
import concurrent
import json
import typing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import flatdict

from ..util import _util

logger = _util.getLogger("exstruct.exstruct.base_exstruct")


class BaseExStruct(abc.ABC):
    """Abstract class from which all structure extractors inherit"""

    _data_type_priorities = {
        "Integer": 1,
        "Boolean": 1,
        "Float": 2,
        "String": 3,
        "object": 3,
    }

    def __init__(
        self,
        data_type_mapping: dict,
        external_id_collection: tuple,
        mapping_delimiter: str = " -> ",
        source_path: Path = None,
        result_path: Path = None,
    ) -> None:
        self._data_type_mapping = data_type_mapping
        self._external_id_collection = external_id_collection
        self._mapping_delimiter = mapping_delimiter

        self._source_path = Path(str(source_path))
        self._source_content = None
        self._ignored_fields = []
        self._result_path = result_path

    def extract_structure(
        self,
        result_path: Path = None,
        ignored_fields: typing.Iterable[str] = None,
        ignore_levels: int = 0,
        sturcture_name: str = None,
    ):
        if ignore_levels < 0:
            err_msg = "'ignore_levels' must be non-negative number"
            raise ValueError(err_msg)

        if ignored_fields:
            self.ignored_fields = ignored_fields
        else:
            self.ignored_fields = []

        if result_path:
            file_path = result_path
        else:
            file_path = self.result_path

        source_content = self.read_source()
        data_structure = self.make_structure(
            source_content, ignored_fields, ignore_levels, sturcture_name
        )

        self.fill_routes(data_structure)

        if file_path:
            self.save_structure_to_file(data_structure, file_path, encoding="utf-8")
            return data_structure
        else:
            return data_structure

    @abc.abstractmethod
    def make_structure(
        self,
        source_content: str,
        ignored_fields: typing.Iterable[str] = None,
        ignore_levels: int = 0,
        structure_name: str = None,
    ):
        pass

    @abc.abstractmethod
    def parse_element(self, element, ignore_levels: int = 0):
        pass

    def _is_ignored_field(self, aliases: typing.Iterable[str]):
        """Check if data type must be ignored

        Args:
            aliases (typing.Iterable[str]): List of data type aliases

        Returns:
            bool
        """
        for ignored_field in self.ignored_fields:
            is_ignored_field = [
                alias.lower().find(ignored_field.lower()) != -1 for alias in aliases
            ]
            if any(is_ignored_field):
                return True

        return False

    def read_source(self, source_path: Path = None, *args, **kwargs):
        if source_path:
            filepath = source_path
        else:
            filepath = self.source_path

        with filepath.open("r", encoding="utf-8") as file:
            self.source_content = file.read()

        return self.source_content

    def fill_routes(
        self,
        data_structure: dict,
    ):
        """Fill paths to content of data types and paths to where they will be saved

        Args:
            data_structure (dict): Description of provided by data-source data
        """
        paths = filter(
            lambda x: x.endswith("@collected_info -> path"),
            tuple(
                flatdict.FlatDict(
                    data_structure, delimiter=self.mapping_delimiter
                ).keys()
            ),
        )
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures_to_process = [
                executor.submit(
                    self.fill_route,
                    path=path,
                    data_structure=data_structure,
                )
                for path in paths
            ]

        for future in concurrent.futures.as_completed(futures_to_process):
            future.result()

    def fill_route(self, path: str, data_structure: dict):
        path_components = "']['".join(path.split(self.mapping_delimiter)[:-1])
        field_content = path.removesuffix(" -> @collected_info -> path")
        self._fill_field(path_components, data_structure, "path", field_content)

        # normalization of mapped name according to python naming rules
        field_content = field_content.split(self.mapping_delimiter)
        field_content[-1] = _util.to_var_name(field_content[-1])
        field_content = " -> ".join(field_content)
        self._fill_field(path_components, data_structure, "mapping", field_content)

    def _fill_field(
        self,
        path_components: str,
        data_structure: dict,
        filed_name: str,
        field_content: str,
    ):
        command = (
            f"data_structure['{path_components}']['{filed_name}'] = '{field_content}'"
        )
        compiled_command = compile(command, __file__, "single")
        exec(compiled_command)

    def save_structure_to_file(
        self, structure: dict, result_path: Path = None, *args, **kwargs
    ):
        with result_path.open("w", **kwargs) as result_file:
            json.dump(structure, result_file, ensure_ascii=False)

    @property
    def data_type_mapping(self):
        return self._data_type_mapping

    @data_type_mapping.setter
    def data_type_mapping(self, new_mapping: dict):
        if isinstance(new_mapping, dict):
            self._data_type_mapping = new_mapping
        else:
            err_msg = f"Expected 'dict', not '{type(new_mapping)}'"
            raise TypeError(err_msg)

    @data_type_mapping.deleter
    def data_type_mapping(self):
        del self._data_type_mapping

    @property
    def external_id_collection(self):
        return self._external_id_collection

    @external_id_collection.setter
    def external_id_collection(self, new_id_collection: tuple):
        self._external_id_collection = tuple(new_id_collection)

    @external_id_collection.deleter
    def external_id_collection(self):
        del self._external_id_collection

    @property
    def source_path(self):
        return self._source_path

    @source_path.setter
    def source_path(self, new_source_path: Path):
        self._source_path = Path(new_source_path)

    @source_path.deleter
    def source_path(self):
        del self._source_path

    @property
    def source_content(self):
        return self._source_content

    @source_content.setter
    def source_content(self, new_content: str):
        self._source_content = str(new_content)

    @source_content.deleter
    def source_content(self):
        del self._source_content

    @property
    def ignored_fields(self):
        return self._ignored_fields

    @ignored_fields.setter
    def ignored_fields(self, new_fields: list | tuple):
        self._ignored_fields = tuple(new_fields)

    @ignored_fields.deleter
    def ignored_fields(self):
        del self._ignored_fields

    @property
    def result_path(self):
        return self._result_path

    @result_path.setter
    def result_path(self, new_result_path: Path):
        self._result_path = Path(new_result_path)

    @result_path.deleter
    def result_path(self):
        del self._result_path

    @property
    def mapping_delimiter(self):
        return self._mapping_delimiter

    @mapping_delimiter.setter
    def mapping_delimiter(self, new_delimiter: str):
        self._mapping_delimiter = str(new_delimiter)

    @mapping_delimiter.deleter
    def mapping_delimiter(self):
        del self._mapping_delimiter

    @property
    def data_type_priorities(self):
        return self._data_type_priorities

    @data_type_priorities.setter
    def data_type_priorities(self, new_priorities: dict):
        if isinstance(new_priorities, dict):
            self._data_type_priorities = new_priorities
        else:
            err_msg = f"Expected 'dict', not '{type(new_priorities)}'"
            raise TypeError(err_msg)

    @data_type_priorities.deleter
    def data_type_priorities(self):
        del self._data_type_priorities
