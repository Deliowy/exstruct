import os
import re
from collections import defaultdict
from pathlib import Path

import jinja2
import more_itertools
from deepdiff import grep

from ..util import util


class ORMClassGenerator(object):
    """Handles generation of ORM classes with '.jinja' templates"""

    _data_type_priorities = {
        "Integer": 1,
        "Boolean": 1,
        "Float": 2,
        "Date": 2,
        "types.TIMESTAMP(timezone=True)": 3,
        "String": 4,
        "LargeBinary": 4,
        "object": 5,
    }

    def __init__(
        self,
        mapping: dict,
        schema: str,
        template_path: Path = "./templates/table_classes.py.jinja",
        mapping_delimiter: str = " -> ",
        classes_save_location: Path = "./_generated_classes/",
    ) -> None:
        template_path = Path(template_path)

        self._mapping = mapping
        self._schema = schema
        self._template_path = Path(template_path)
        self._mapping_delimiter = mapping_delimiter
        self._classes_save_location = Path(classes_save_location)

        self._classes = defaultdict()
        self._relationships = defaultdict()
        self._foreign_keys = defaultdict()
        self._tables_info = defaultdict()
        self._assoc_tables_names = defaultdict()
        self._external_ids = defaultdict()

    def generate(self):
        self.list_classes_to_generate(self.mapping)
        self.reflect_relationships()
        self.make_assoc_tables_names()
        self.get_external_ids()

        content = {}
        content["table_schema"] = self.schema
        content["tables"] = self.classes
        content["relationships"] = self.relationships
        content["foreign_keys"] = self.foreign_keys
        content["assoc_tables_names"] = self.assoc_tables_names
        content["external_ids"] = self.external_ids
        content["tables_info"] = self.tables_info

        rendered_classes = self.render(content)

        return self.save_rendered_classes_to_file(rendered_classes)

    def list_classes_to_generate(self, mapping: dict):
        for data_type_name, data_type in mapping.items():
            self.classes.setdefault(data_type_name, {})
            self.relationships.setdefault(data_type_name, [])
            self.foreign_keys.setdefault(data_type_name, [])

            data_type_elements = {
                child_key: data_type_child
                for child_key, data_type_child in data_type.items()
            }
            data_type_collected_info = data_type_elements.pop("@collected_info")

            data_type_elements_settings = {
                element_key: data_type_elements[element_key]["@collected_info"]
                for element_key in data_type_elements
            }

            self.tables_info[data_type_name] = data_type_collected_info["annotation"]

            is_table = lambda key: data_type_elements_settings[key]["type"] == "object"

            columns_names, table_names = more_itertools.partition(
                is_table, data_type_elements
            )
            columns_names, table_names = tuple(columns_names), tuple(table_names)

            self.__map_columns(
                {
                    column_name: data_type_elements[column_name]
                    for column_name in columns_names
                }
            )

            data_type_foreign_keys = []

            for child_table_name in table_names:
                data_type_foreign_keys.append(child_table_name)
                self.list_classes_to_generate(
                    {child_table_name: data_type_elements[child_table_name]}
                )

            self.foreign_keys[data_type_name] = list(
                set(self.foreign_keys[data_type_name]) | set(data_type_foreign_keys)
            )

            self.relationships[data_type_name] = list(
                set(self.relationships[data_type_name]) | set(table_names)
            )

    def __map_columns(self, columns: dict):
        for _, column_settings in columns.items():
            mapping = column_settings["@collected_info"]["mapping"]
            # get element's parent name
            column_table = mapping.split(self.mapping_delimiter)[-2]
            column_name = mapping.split(self.mapping_delimiter)[-1]
            if column_name not in self.classes[column_table]:
                self.classes[column_table].update({column_name: column_settings})
            else:
                old_type = self.classes[column_table][column_name]["@collected_info"][
                    "type"
                ]
                new_type = column_settings["@collected_info"]["type"]
                if (
                    self._data_type_priorities[new_type]
                    > self._data_type_priorities[old_type]
                ):
                    self.classes[column_table][column_name]["@collected_info"][
                        "type"
                    ] = new_type

    def reflect_relationships(self):
        for table_name in self.relationships:
            related_tables = (
                self.relationships
                | grep(table_name, match_string=True, case_sensitive=True)
            ).get("matched_values", [])

            for related_table_path in related_tables:
                related_table_name = re.match(".*'(\S+)'.*", related_table_path)[1]

                if related_table_name not in self.relationships[table_name]:
                    self.relationships[table_name].append(related_table_name)

                if table_name not in self.relationships[related_table_name]:
                    self.relationships[related_table_name].append(table_name)

    def make_assoc_tables_names(self):
        for table_name in self.foreign_keys:
            for foreign_key in self.foreign_keys[table_name]:
                assoc_table_name = f"at_{table_name}_{foreign_key}"
                self.assoc_tables_names[(table_name, foreign_key)] = assoc_table_name[
                    :63
                ]

    def get_external_ids(self):
        for table_name, table_content in self.classes.items():
            external_ids_names = filter(
                lambda item_name: table_content[item_name]["@collected_info"][
                    "external_id"
                ],
                table_content,
            )
            self.external_ids[table_name] = tuple(external_ids_names)

    def render(self, content: dict):
        loader = jinja2.loaders.FileSystemLoader(self.template_path.parent)
        environment = jinja2.Environment(
            loader=loader, trim_blocks=True, keep_trailing_newline=True
        )
        template = environment.get_or_select_template(self.template_path.name)
        rendered_classes = template.render(content)
        return rendered_classes

    def save_rendered_classes_to_file(
        self, rendered_classes: str, save_location: Path = None
    ):
        if save_location:
            classes_save_location = save_location
        else:
            classes_save_location = self.classes_save_location

        os.makedirs(classes_save_location, exist_ok=True)
        open(classes_save_location.joinpath("__init__.py"), "a").close()
        classes_filepath = classes_save_location.joinpath(
            f"{self.schema}_generated_classes.py"
        )
        with classes_filepath.open("w", encoding="utf-8") as classes_file:
            classes_file.write(rendered_classes)
        return classes_filepath

    @property
    def mapping(self):
        return self._mapping

    @mapping.setter
    def mapping(self, new_mapping: dict):
        if isinstance(new_mapping, dict):
            self._mapping = new_mapping
        else:
            err_msg = f"Expected 'dict', not '{type(new_mapping)}'"
            raise TypeError(err_msg)

    @mapping.deleter
    def mapping(self):
        del self._mapping

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, new_schema: str):
        self._schema = str(new_schema)

    @schema.deleter
    def schema(self):
        del self._schema

    @property
    def template_path(self):
        return self._template_path

    @template_path.setter
    def template_path(self, new_template_path: Path):
        self._template_path = Path(new_template_path)

    @template_path.deleter
    def template_path(self):
        del self._template_path

    @property
    def archive_template_path(self):
        return self._archive_template_path

    @property
    def mapping_delimiter(self):
        return self._mapping_delimiter

    @mapping_delimiter.setter
    def mapping_delimiter(self, new_mapping_delimiter: str):
        self._mapping_delimiter = str(new_mapping_delimiter)

    @mapping_delimiter.deleter
    def mapping_delimiter(self):
        del self._mapping_delimiter

    @property
    def classes_save_location(self):
        return self._classes_save_location

    @classes_save_location.setter
    def classes_save_location(self, new_save_location: Path):
        self._classes_save_location = Path(new_save_location)

    @classes_save_location.deleter
    def classes_save_location(self):
        del self._classes_save_location

    @property
    def classes(self):
        return self._classes

    @property
    def relationships(self):
        return self._relationships

    @property
    def foreign_keys(self):
        return self._foreign_keys

    @property
    def tables_info(self):
        return self._tables_info

    @property
    def external_ids(self):
        return self._external_ids

    @property
    def assoc_tables_names(self):
        return self._assoc_tables_names
