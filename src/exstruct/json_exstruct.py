import json
import typing
from pathlib import Path

import jsonschema
from deepdiff import DeepDiff

from ..util import util
from .base_exstruct import BaseExStruct

logger = util.getLogger("exstruct.exsturct.json_exstruct")


class JSONSchemaExStruct(BaseExStruct):
    """Extractor of data structure from JSON schema"""

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
        self.__ref_resolver = None

    def make_structure(
        self,
        source_content: str,
        ignored_fields: typing.Iterable[str] = None,
        ignore_levels: int = 0,
        structure_name: str = None,
    ):
        json_schema_dict = json.loads(source_content)

        ref_resolver = jsonschema.validators.RefResolver.from_schema(json_schema_dict)
        json_elements = json_schema_dict["definitions"]
        structure = {}
        for element_name in json_elements:
            structure[element_name] = self.parse_element(
                element_name, json_elements[element_name], ref_resolver
            )

        return structure

    def parse_element(
        self,
        schema_name: str,
        schema_type: dict,
        ref_resolver: jsonschema.validators.RefResolver,
    ):
        collected_info_settings = {}

        if schema_type.get("$ref", None):
            schema_type = self.resolve_ref(schema_type, ref_resolver)

        collected_info_settings["annotation"] = repr(schema_type.get("description", ""))

        collected_info_settings["aliases"] = [schema_name]

        if self._is_ignored_field(collected_info_settings["aliases"]):
            collected_info_settings["collected_info_type"] = None
        else:
            collected_info_settings["collected_info_type"] = "V"

        collected_info_settings["type"] = self.data_type_mapping[schema_type["type"]]

        collected_info_settings["occurence"] = False

        collected_info_settings["external_id"] = (
            True if schema_name.lower() in self.external_id_collection else False
        )

        collected_info_settings["path"] = ""
        collected_info_settings["mapping"] = ""

        result = {}
        child_elements = schema_type.get("properties", None)
        if child_elements:
            for child_element_name in child_elements:
                result[child_element_name] = self.parse_element(
                    child_element_name, child_elements[child_element_name], ref_resolver
                )

        result["@collected_info"] = collected_info_settings

        return result

    def resolve_ref(
        self, schema_ref: dict, ref_resolver: jsonschema.validators.RefResolver
    ):
        ref = schema_ref["$ref"]
        schema_ref_name, schema_content = ref_resolver.resolve(ref)
        schema_name = schema_ref_name.split("/")[-1]

        return schema_content

    @property
    def ref_resolver(self):
        return self.__ref_resolver

    @ref_resolver.setter
    def ref_resolver(self, new_ref_resolver: jsonschema.validators.RefResolver):
        if isinstance(new_ref_resolver, jsonschema.validators.RefResolver):
            self.__ref_resolver = new_ref_resolver
        else:
            err_msg = f"Expected 'jsonschema.validators.RefResolver', not '{type(new_ref_resolver)}'"
            raise TypeError(err_msg)

    @ref_resolver.deleter
    def ref_resolver(self):
        del self.__ref_resolver


class JSONExStruct(BaseExStruct):
    """Extractor of data sturcture from JSON"""

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
        if ignored_fields:
            self.ignored_fields = ignored_fields
        else:
            self.ignored_fields = []

        content_json = json.loads(source_content)

        data_structure = {}

        if isinstance(content_json, (tuple, list)):
            for json_element in content_json:
                element_structure = self.parse_json(json_element, structure_name)

                element_structure_diff = DeepDiff(data_structure, element_structure)

                if element_structure_diff and data_structure:
                    self.update_structure(
                        data_structure,
                        element_structure,
                        element_structure_diff,
                    )
                else:
                    data_structure.update(self.parse_json(json_element, structure_name))

        else:
            data_structure = self.parse_json(content_json, structure_name)

        return data_structure

    def parse_json(self, json_entry: dict, structure_name: str = None):
        data_structure = {}

        if structure_name is None:
            structure_name = "entry"

        if isinstance(json_entry, dict) and len(json_entry) > 1:
            json_entry = {structure_name: json_entry}

        data_structure.update(self.parse_element(json_entry))

        return data_structure

    def parse_element(self, element: dict):
        try:
            (element_name, element_content) = tuple(*element.items())
            collected_info_settings = {}

            collected_info_settings["annotation"] = ""
            collected_info_settings["aliases"] = [element_name]

            if self._is_ignored_field(collected_info_settings["aliases"]):
                collected_info_settings["collected_info_type"] = None
            else:
                collected_info_settings["collected_info_type"] = "V"

            collected_info_settings["type"] = (
                "object"
                if isinstance(element_content, (dict, list))
                else self.data_type_mapping[element_content.__class__.__name__]
            )
            collected_info_settings["occurence"] = False

            collected_info_settings["external_id"] = (
                True if element_name.lower() in self.external_id_collection else False
            )

            collected_info_settings["path"] = ""
            collected_info_settings["mapping"] = ""

            result = {}

            if isinstance(element_content, dict):
                self._parse_child_element(element_content, result)

            elif isinstance(element_content, (list, tuple)):
                for child_element in element_content:
                    self._parse_child_element(child_element, result)

            result["@collected_info"] = collected_info_settings

            return {element_name: result}

        except Exception as err:
            err_msg = f"Ошибка {err.args} при парсинге поля '{element_name}' элемента {element}"
            logger.error(err_msg)

    def _parse_child_element(self, element_content, result):
        for name, value in element_content.items():
            child_structure = self.parse_element({name: value})
            result.update(child_structure)

    def update_structure(
        self, structure: dict, entry_structure: dict, fields_to_update: DeepDiff
    ):
        for field in fields_to_update:
            if field == "values_changed":
                affected_fields = fields_to_update[field]
                for affected_path in affected_fields:
                    affected_field = affected_fields[affected_path]
                    if (
                        self.data_type_priorities[affected_field["new_value"]]
                        > self.data_type_priorities[affected_field["old_value"]]
                    ):
                        updated_value = affected_field["new_value"]
                    else:
                        updated_value = affected_field["old_value"]

                    path = affected_path.removeprefix("root")
                    command = compile(
                        f"structure{path} = '{updated_value}'",
                        __file__,
                        "single",
                    )
                    exec(command)
            elif field == "dictionary_item_added":
                affected_fields = fields_to_update[field]
                for affected_path in affected_fields:
                    path = affected_path.removeprefix("root")
                    command = compile(
                        f"structure{path} = entry_structure{path}",
                        __file__,
                        "single",
                    )
                    exec(command)
