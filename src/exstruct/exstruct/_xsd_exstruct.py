import typing
from pathlib import Path

import xmlschema

from .. import util
from ._base_exstruct import BaseExStruct


class XSDExStruct(BaseExStruct):
    """Extractor of data structure from XSD schema"""

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
        structure_name: str = None,
    ):
        if ignore_levels < 0:
            err_msg = "'ignore_levels' must be non-negative number"
            raise ValueError(err_msg)

        if ignored_fields:
            self.ignored_fields = ignored_fields
        else:
            self.ignored_fields = []

        xsd_schema = xmlschema.XMLSchema(source_content)

        data_structure = {}

        for root_element in xsd_schema.iterchildren():
            if ignore_levels:
                data_structure.update(self.parse_element(root_element, ignore_levels - 1))
                data_structure.pop("@collected_info", None)
            else:
                data_structure.update({root_element.local_name: self.parse_element(root_element)})

        return data_structure

    def parse_element(self, element: xmlschema.XsdElement, ignore_levels: int = 0):
        if ignore_levels < 0:
            err_msg = "'ignore_levels' must be non-negative number"
            raise ValueError(err_msg)

        collected_info_settings = {}

        self._parse_element_common_info(element, collected_info_settings)

        collected_info_settings["type"] = (
            self.data_type_mapping[self.get_data_type(element.type)]
            if (element.type.is_simple() or element.type.has_simple_content())
            else "object"
        )

        if element.type.is_simple() or element.type.has_simple_content():
            result = self._parse_simple_element(element, collected_info_settings, ignore_levels)
        else:
            result = self._parse_complex_element(element, collected_info_settings, ignore_levels)

        return result

    def _parse_element_common_info(self, element, collected_info_settings):
        collected_info_settings["annotation"] = repr(str(element.annotation))

        element_aliases = [
            util.normalize_str(element.name),
            util.normalize_str(element.local_name),
            util.normalize_str(element.prefixed_name),
            util.normalize_str(element.qualified_name),
        ]

        if element.type:
            element_aliases.extend(
                [
                    util.normalize_str(element.type.name),
                    util.normalize_str(element.type.local_name),
                    util.normalize_str(element.type.prefixed_name),
                    util.normalize_str(element.type.qualified_name),
                ]
            )

        collected_info_settings["aliases"] = tuple(set(filter(None, element_aliases)))

        collected_info_settings["external_id"] = (
            True if element.local_name.lower() in self.external_id_collection else False
        )

        if self._is_ignored_field(collected_info_settings["aliases"]):
            collected_info_settings["collected_info_type"] = None
        else:
            collected_info_settings["collected_info_type"] = "V"

        try:
            collected_info_settings["occurence"] = bool(element.min_occurs) or not (
                element.parent and (element.parent.model == "choice")
            )
        except AttributeError as err:
            collected_info_settings["occurence"] = True

        collected_info_settings["path"] = ""
        collected_info_settings["mapping"] = ""

    def _parse_simple_element(self, element: xmlschema.XsdElement, collected_info_settings: dict, ignore_levels: int):
        result = {}

        group_model = {"sequence": self.converter_sequence, "all": self.converter_all, "choice": self.converter_choice}

        if group_model[element.parent.model] == "table":
            collected_info_settings["type"] = "object"
            collected_info_settings["value_column"] = True
        else:
            collected_info_settings["type"] = self.data_type_mapping[self.get_data_type(element.type)]
            collected_info_settings["value_column"] = False

        result["@collected_info"] = collected_info_settings

        return result

    def _parse_complex_element(self, element: xmlschema.XsdElement, collected_info_settings: dict, ignore_levels: int):
        result = {}
        collected_info_settings["value_column"] = False
        result["@collected_info"] = collected_info_settings

        # Проверка на бесконечную рекурсию
        if element.parent and (element.type == element.parent.parent):
            return result

        for child in element.iterchildren():
            if ignore_levels:
                result.update(self.parse_element(child, ignore_levels - 1))
                result.pop("@collected_info")
            else:
                result.update({child.local_name: self.parse_element(child)})

        return result

    def get_data_type(
        self,
        type: xmlschema.validators.XsdComplexType | xmlschema.validators.XsdElement,
    ):
        if hasattr(type, "base_type") and type.base_type is not None:
            return self.get_data_type(type.base_type)
        return type.local_name
