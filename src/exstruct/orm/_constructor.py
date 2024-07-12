import datetime
import importlib
import itertools
import typing
import warnings

import more_itertools
import sqlalchemy.orm
import sqlalchemy.util

from .. import util


class ORMObjectsConstructor(object):
    """Consturctor of ORM classes objects"""

    def __init__(
        self,
        imported_modules: list[str] = None,
        default_orm_package: str = "_generated_classes",
        default_schema: str = None,
    ) -> None:
        """Initialize instance of `ORMObjectConstructor`

        Args:
            imported_modules (list[str], optional): list of modules where ORM classes are described. Defaults to None.
            default_orm_package (str, optional): Default name for sub-module containing generated ORM classes. Defaults to "_generated_classes".
            default_schema (str, optional): Default schema to. Defaults to None.
        """
        self.logger = util.getLogger(f"{self.__module__}.{self.__class__.__name__}")
        self._imported_modules = imported_modules if imported_modules else []
        self._default_orm_package_name = default_orm_package
        self._default_schema = default_schema

        self.import_modules(self._imported_modules, self._default_orm_package_name)

        self._used_classes = set()

    def clear_cache(self):
        """Clear created instances of ORM classes"""
        for cls in self._used_classes:
            cls._instances.clear()

    def construct(self, document: dict | typing.Iterable[dict], schema: str = None):
        """Make ORM class instance(s), using given document(s) and schema

        Args:
            document (dict | typing.Iterable[dict]): document(s) matching existing ORM classes
            schema (str, optional): Schema to which created ORM classes are attributed. Defaults to None.

        Returns:
            list: created ORM objects
        """
        # constructed_objects CAN contain duplicate values
        if isinstance(document, (list, tuple)):
            constructed_objects = list(
                self._construct(document=document, schema=schema)
            )
        else:
            constructed_objects = [self._construct(document=document, schema=schema)]

        result = []
        result_ids = []
        for orm_object in more_itertools.collapse(
            constructed_objects, base_type=sqlalchemy.orm.DeclarativeMeta
        ):
            if id(orm_object) not in result_ids:
                result.append(orm_object)
                result_ids.append(id(orm_object))
        return result

    def _construct(self, document: dict | typing.Iterable[dict], schema: str = None):
        """Make ORM class instance(s), using given document(s) and schema

        Args:
            document (dict | typing.Iterable[dict]): document(s) matching existing ORM classes
            schema (str, optional): Schema to which created ORM classes are attributed. Defaults to None.

        Raises:
            ValueError: Raised if document content is not incapsulated in `dict` with 1 element (`{'doc':document_content}`)

        Returns:
            typing.Iterable: created ORM objects
        """
        if isinstance(document, dict) and len(document) > 1:
            err_msg = "Document content must be incapsulated in 'dict' with 1 element. e.g. {'document':document_content}"
            raise ValueError(err_msg)

        if schema is None and self._default_schema:
            schema = self._default_schema

        if isinstance(document, (list, tuple, map)):
            return itertools.starmap(
                self._construct, itertools.product(document, (schema,))
            )

        module_name = f"{schema}{self._default_orm_package_name}"
        self.import_module(
                module_name,
                self._default_orm_package_name,
            )

        root_object = self.create_object(module_name, *document.items())

        return root_object

    def create_object(
        self,
        module_name: str,
        object_args: tuple[str, ...],
        parent: sqlalchemy.orm.DeclarativeMeta = None,
    ):
        """Transform document into ORM object

        Args:
            module_name (str): Module containing ORM classes
            object_args (tuple[str, Any]): name of ORM class and values of it's properties
            parent (sqlalchemy.orm.DeclarativeMeta, optional): Parent of created ORM object . Defaults to None.

        Returns:
            sqlalchemy.orm.DeclarativeMeta: ORM object containing document's data
        """
        object_name, object_elements = object_args

        if isinstance(object_elements, (list, tuple)):
            return [
                self.create_object(module_name=module_name, object_args=item)
                for item in itertools.zip_longest(
                    tuple(), object_elements, fillvalue=object_name
                )
            ]

        columns, children = {}, {}
        for key, value in object_elements.items():
            if isinstance(value, (dict, list)):
                children[key] = value
            else:
                if value is not None:
                    columns[util.to_var_name(key)] = self._prepare_value(value)

        new_object = self._create_object(module_name, object_name, **columns)

        self._used_classes.add(new_object.__class__)

        for child in children.items():
            if isinstance(child[1], list):
                child_orm = []
                for item in child[1]:
                    child_orm.append(self.create_object(module_name, (child[0], item)))
                self._add_relationship(new_object, child_orm) if child_orm else None
            else:
                self.create_object(module_name, child, new_object)

        if parent is not None:
            self._add_relationship(parent, new_object)

        return new_object

    def _create_object(self, module_name: str, class_name: str, **columns):
        """Handles actual construction of ORM object

        Args:
            module_name (str): Module containing definitions of ORM classes
            class_name (str): Name of ORM class

        Returns:
            sqlalchemy.orm.DeclarativeMeta: Instance of ORM class `class_name`
        """
        command_arguments = []
        for name, value in columns.items():
            command_arguments.append(f"{name} = {value}")
        command_arguments_str = ", ".join(command_arguments)
        command = f"result = {module_name}.{util.to_var_name(class_name)}({command_arguments_str})"
        compiled_command = compile(command, __file__, "single")

        loc = {}
        exec(compiled_command, globals(), loc)

        return loc["result"]

    def _prepare_value(
        self, value: str | int | float | datetime.date | datetime.datetime
    ):
        """Prepare value for use in dynamicaly generated ORM object instantiation

        Args:
            value (str | int | float | datetime.date | datetime.datetime): Value to prepare

        Returns:
            str | int | float: Prepared value
        """
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
        child: sqlalchemy.orm.DeclarativeMeta | list[sqlalchemy.orm.DeclarativeMeta],
    ):
        """Add relationship between two ORM objects

        Args:
            parent (sqlalchemy.orm.DeclarativeMeta): Parent of ORM relationship
            child (sqlalchemy.orm.DeclarativeMeta): Child of ORM relationship
        """
        if not isinstance(child, (list, tuple)):
            children = [child]
        else:
            children = child
        relationship_name = f"relationship_{children[0].__cls_name__}"
        relationship = parent.__getattribute__(relationship_name)
        relationship_ids = {id(item) for item in relationship}
        for child in children:
            if id(child) not in relationship_ids:
                relationship.append(child)
                relationship_ids.add(id(child))

    def import_module(self, module_name: str, package_name: str = None):
        """Add module to global namespace

        Args:
            module_name (str): Module to import
            package_name (str, optional): Sub-module where module lies. Defaults to None.
        """
        if globals().get(module_name):
            err_msg = f"Module {module_name} is already imported"
            warnings.warn(err_msg)
            globals()[module_name] = importlib.reload(globals()[module_name])
            return globals()[module_name]
        
        if module_name not in self._imported_modules:
            self._imported_modules.append(module_name)

        if package_name is None:
            package_name = self._default_orm_package_name

        importlib.invalidate_caches()
        globals()[module_name] = importlib.import_module(
            f".{module_name}", package_name
        )
        return globals()[module_name]

    def import_modules(
        self, modules_names: typing.Iterable[str], package_name: str = None
    ):
        """Add modules to global namespace

        Args:
            modules_names (typing.Iterable[str]): List of modules to import
            package_name (str, optional): Module where modules-to-import lie. Defaults to None.
        """
        for module_name in modules_names:
            self.import_module(module_name, package_name)

    @property
    def imported_modules(self):
        return self._imported_modules

    @imported_modules.setter
    def imported_modules(self, new_imported_modules: typing.Iterable[str]):
        self._imported_modules.clear()
        self.import_modules(new_imported_modules)
