import datetime
import importlib
import itertools
import typing
import warnings

import sqlalchemy.orm
import sqlalchemy.util

from ..util import _util

logger = _util.getLogger("exstruct.orm.constructor")

class ORMObjectsConstructor(object):
    """Consturctor of ORM classes objects"""

    def __init__(
        self,
        mapping: dict,
        imported_modules: list[str] = None,
        default_orm_package: str = "_generated_classes",
        default_schema: str = None,
    ) -> None:
        # FIXME Use mapping or remove it
        self._mapping = mapping

        self._imported_modules = imported_modules if imported_modules else []
        self._default_orm_package_name = default_orm_package
        self._default_schema = default_schema

        self.import_modules(self._imported_modules, self._default_orm_package_name)

        self._used_classes = set()

    def clear_instances(self):
        for cls in self._used_classes:
            cls._instances.clear()

    def construct(self, document: dict | typing.Iterable[dict], schema: str = None):
        # constructed_objects CAN contain duplicate values
        constructed_objects = self._construct(document, schema)

        result = []
        result_ids = []
        for object in constructed_objects:
            if id(object) not in result_ids:
                result.append(object)
                result_ids.append(id(object))
        return result

    def _construct(self, document: dict | typing.Iterable[dict], schema: str = None):
        if isinstance(document, dict) and len(document) > 1:
            err_msg = "Document content must be incapsulated in 'dict' with 1 element. e.g. {'document':document_content}"
            raise ValueError(err_msg)

        if schema is None and self._default_schema:
            schema = self._default_schema

        if isinstance(document, (list, tuple)):
            return itertools.starmap(
                self._construct, itertools.product(document, (schema,))
            )

        module_name = f"{schema}{self._default_orm_package_name}"
        if module_name not in self.imported_modules:
            self.import_module(
                module_name,
                self._default_orm_package_name,
            )

        root_object = self.create_object(module_name, *document.items())

        return root_object

    def create_object(
        self,
        module_name: str,
        object_args: tuple,
        parent: sqlalchemy.orm.DeclarativeMeta = None,
    ):
        object_name, object_elements = object_args

        columns, children = {}, {}
        for key, value in object_elements.items():
            if isinstance(value, (dict, list)):
                children[key] = value
            else:
                if value is not None:
                    columns[key] = self._prepare_value(value)

        new_object = self._create_object(module_name, object_name, **columns)

        self._used_classes.add(new_object.__class__)

        for child in children.items():
            if isinstance(child[1], list):
                for item in child[1]:
                    self.create_object(module_name, (child[0], item), new_object)
            else:
                self.create_object(module_name, child, new_object)

        if parent is not None:
            self._add_relationship(parent, new_object)

        return new_object

    def _create_object(self, module_name: str, class_name: str, **columns):
        command_arguments = []
        for name, value in columns.items():
            command_arguments.append(f"{name} = {value}")
        command_arguments_str = ", ".join(command_arguments)
        command = f"result = {module_name}.{_util.to_var_name(class_name)}({command_arguments_str})"
        compiled_command = compile(command, __file__, "single")

        loc = {}
        exec(compiled_command, globals(), loc)

        return loc["result"]

    def _prepare_value(self, value):
        if isinstance(value, str):
            unquoted_value = value.replace('"', "''")
            prepared_value = (
                f'r""" {unquoted_value} """' if '"' in value else repr(value)
            )
        elif isinstance(value, datetime.datetime):
            str_datetime = str(value)
            prepared_value = f"datetime.datetime.fromisoformat('{str_datetime}')"
        elif isinstance(value, datetime.date):
            str_date = str(value)
            prepared_value = f"datetime.date.fromisoformat('{str_date}')"
        else:
            prepared_value = value
        return prepared_value

    def _add_relationship(
        self,
        parent: sqlalchemy.orm.DeclarativeMeta,
        child: sqlalchemy.orm.DeclarativeMeta,
    ):
        relationship_name = f"relationship_{child.__cls_name__}"
        relationship = parent.__getattribute__(relationship_name)
        relationship_ids = [id(item) for item in relationship]
        if id(child) not in relationship_ids:
            relationship.append(child)

    def import_module(self, module_name: str, package_name: str = None):
        if globals().get(module_name, None):
            err_msg = f"Module {module_name} is already imported"
            warnings.warn(err_msg)
            if module_name not in self._imported_modules:
                self._imported_modules.append(module_name)

        if package_name is None:
            package_name = self._default_orm_package_name

        importlib.invalidate_caches()
        globals()[module_name] = importlib.import_module(
            f".{module_name}", package_name
        )
        self._imported_modules.append(module_name)

    def import_modules(
        self, modules_names: typing.Iterable[str], package_name: str = None
    ):
        for module_name in modules_names:
            self.import_module(module_name, package_name)

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

    @property
    def imported_modules(self):
        return self._imported_modules

    @imported_modules.setter
    def imported_modules(self, new_imported_modules: typing.Iterable[str]):
        self._imported_modules.clear()
        self.import_modules(new_imported_modules)
