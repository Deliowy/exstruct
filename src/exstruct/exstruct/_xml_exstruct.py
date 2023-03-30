import shutil
import tempfile
import typing
from pathlib import Path

import xmlschema
from deepdiff import DeepDiff

from ..util import _util
from ._base_exstruct import BaseExStruct

logger = _util.getLogger("exstruct.exstruct.xml_exstruct")


class XMLExStruct(BaseExStruct):
    """Extractor of data structure from XML document"""

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

        data_structure = {}

        xml_document = xmlschema.XmlDocument(source_content, validation="skip")

        tmpdir = Path(tempfile.mkdtemp())
        try:
            xsd_schema = xmlschema.XMLSchema(xml_document.namespace)
            xsd_schema.export(tmpdir, save_remote=True)
            xsd_schema_filepath = tmpdir.joinpath(xsd_schema.url)
            with xsd_schema_filepath.open("r", encoding="utf-8") as file:
                xsd_schema_content = file.read()
            data_structure = self.__from_xsd(xsd_schema_content)
        except Exception as err:
            logger.error(err)
            msg = "Не удалось получить схему данных. Структура будет построена только на основе .xml-файла."
            print(msg)
            logger.info(msg)

            xml_json = xml_document.decode()
            data_structure = self.__from_json(xml_json, structure_name)

        finally:
            shutil.rmtree(tmpdir)

        return data_structure

    def __from_json(self, content_json: dict, structure_name: str):
        data_structure = {}

        if isinstance(content_json, (tuple, list)):
            for json_element in content_json:
                element_structure = self.parse_json(json_element, structure_name)

                element_structure_diff = DeepDiff(data_structure, element_structure)

                if element_structure_diff and data_structure:
                    self.__json_update_structure(
                        data_structure,
                        element_structure,
                        element_structure_diff,
                    )
                else:
                    data_structure.update(self.parse_json(json_element, structure_name))

        else:
            data_structure = self.parse_json(content_json, structure_name)

        return data_structure

    def parse_json(self, json_entry: dict, structure_name: str):
        data_structure = {}

        if structure_name is None:
            structure_name = "entry"

        if isinstance(json_entry, dict) and len(json_entry) > 1:
            json_entry = {structure_name: json_entry}

        (json_entry_name,) = tuple(json_entry.keys())

        data_structure.update(
            {
                json_entry_name: self.parse_element(
                    json_entry[json_entry_name], json_entry_name
                )
            }
        )

        return data_structure

    def parse_element(self, export_type: dict, key: str):
        try:
            collected_info_settings = {}

            collected_info_settings["annotation"] = ""
            collected_info_settings["aliases"] = [key]
            collected_info_settings["collected_info_type"] = "V"

            collected_info_settings["type"] = (
                "object"
                if isinstance(export_type, (dict, list, tuple))
                else self.data_type_mapping[export_type.__class__.__name__]
            )
            collected_info_settings["occurence"] = False

            collected_info_settings["external_id"] = (
                True if key.lower() in self.external_id_collection else False
            )

            collected_info_settings["path"] = ""
            collected_info_settings["mapping"] = ""

            result = {}

            if isinstance(export_type, (list, tuple)):
                for child_element in export_type:
                    child_element_structure = self.parse_element(child_element, key)
                    element_structure_diff = DeepDiff(result, child_element_structure)
                    if element_structure_diff and result:
                        self.__json_update_structure(
                            result, child_element_structure, element_structure_diff
                        )
                    else:
                        result.update(child_element_structure)
            elif isinstance(export_type, dict):
                result.update(
                    {
                        child_element_name: self.parse_element(
                            export_type[child_element_name], child_element_name
                        )
                        for child_element_name in export_type
                    }
                )

            result["@collected_info"] = collected_info_settings

            return result

        except Exception as err:
            err_msg = f"Ошибка {err} при парсинге поля '{key}' элемента {export_type}"
            logger.error(err_msg)

    def __json_update_structure(
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

    def __from_xsd(self, source_content: str, ignored_fields: typing.Iterable[str]):
        if ignored_fields:
            self.ignored_fields = ignored_fields
        else:
            self.ignored_fields = []

        xsd_schema = xmlschema.XMLSchema(source_content)

        data_structure = {}

        for root_element in xsd_schema.iterchildren():
            data_structure.update(
                {root_element.local_name: self.xsd_parse_element(root_element)}
            )

        return data_structure

    def xsd_parse_element(self, element: xmlschema.validators.XsdElement):
        collected_info_settings = {}

        collected_info_settings["annotation"] = repr(str(element.annotation))

        element_aliases = [
            _util.normalize_str(element.name),
            _util.normalize_str(element.local_name),
            _util.normalize_str(element.prefixed_name),
            _util.normalize_str(element.qualified_name),
        ]

        if element.type:
            element_aliases.extend(
                [
                    _util.normalize_str(element.type.name),
                    _util.normalize_str(element.type.local_name),
                    _util.normalize_str(element.type.prefixed_name),
                    _util.normalize_str(element.type.qualified_name),
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

        collected_info_settings["type"] = (
            self.data_type_mapping[self.__get_data_type(element.type)]
            if element.type.is_simple() or element.type.has_simple_content()
            else "object"
        )

        try:
            collected_info_settings["occurence"] = bool(element.min_occurs) or not (
                element.parent and (element.parent.model == "choice")
            )
        except AttributeError as err:
            collected_info_settings["occurence"] = True

        collected_info_settings["path"] = ""
        collected_info_settings["mapping"] = ""

        result = {}
        result["@collected_info"] = collected_info_settings

        # Проверка на бесконечную рекурсию
        if element.parent and (element.type == element.parent.parent):
            return result

        element_children = element.iterchildren()

        for child in element_children:
            result.update({child.local_name: self.xsd_parse_element(child)})

        return result

    def __get_data_type(
        self,
        type: xmlschema.validators.XsdComplexType | xmlschema.validators.XsdElement,
    ):
        if hasattr(type, "base_type") and type.base_type is not None:
            return self.__get_data_type(type.base_type)
        return type.local_name
