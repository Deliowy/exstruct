import pathlib
import tempfile
import typing

import dateutil
import dateutil.parser
import umsgpack

from ..util import _util

logger = _util.getLogger("exstruct.extractor.extractor")


# DONE TODO IMPROVE
class DataExtractor(object):
    """Extractor of data from processed document"""

    def __init__(
        self,
        mapping: dict = None,
        mapping_delimiter: str = " -> ",
        root_prefix: str = None,
        *args,
        **kwargs,
    ) -> None:
        self._mapping = mapping
        self._mapping_delimiter = mapping_delimiter
        self._root_prefix = root_prefix

    def extract_from_file(self, file: typing.BinaryIO):
        """Extract data from given file containing `umsgpack` dumps. File will be read from beginning 'till getting `InsufficientDataException`.
        Extracted data is dumped in temporary file in same directory with given one.
        Deletition of file with extracted data must be handled separately.

        Args:
            file (typing.BinaryIO): File-like object, containing `umsgpack` dumps.

        Returns:
            tempfile._TemporaryFileWrapper: File-like object, containing data extracted from given file. Extracted data is dumped using `umsgpack`.
        """
        extracted_data_file = tempfile.TemporaryFile(
            dir=pathlib.Path(file.name).parent, delete=False, suffix=".msgpack"
        )

        processed_data = 0
        file.seek(0)
        try:
            while True:
                data_batch = umsgpack.load(file)
                if data_batch:
                    extracted_data_batch = tuple(self.extract(data_batch))
                    processed_data += len(extracted_data_batch)
                    umsgpack.dump(extracted_data_batch, extracted_data_file)
                else:
                    logger.error("Empty batch")
        except umsgpack.InsufficientDataException:
            info_msg = f"Finished extracting data. Processed {processed_data} documents"
            logger.info(info_msg)

        return extracted_data_file

    def extract(self, content: dict | typing.Iterable[dict]):
        """Extract data from `content` based on mapping

        Args:
            content (dict | typing.Iterable[dict]): Document data or multiple instances of them

        Raises:
            ValueError: Raised if passed data is not tree

        Returns:
            dict: Extracted from passed document data
        """
        if isinstance(content, dict) and len(content) > 1:
            err_msg = "Data type elements must be incapsulated in a dict"
            raise ValueError(err_msg)

        if isinstance(content, (list, tuple, map)):
            return map(self.extract, content)

        data_container = content[self.root_prefix] if self.root_prefix else content
        processed_data_type_name = list(data_container.keys()).pop()

        processed_data_type = self.mapping[processed_data_type_name]
        processed_data_type_settings = processed_data_type["@collected_info"]
        data_type_mapping_name = self._data_type_mapping_name(
            processed_data_type_settings
        )

        extracted_data = {}

        if processed_data_type_settings["collected_info_type"] == "V":
            extracted_data[data_type_mapping_name] = self._extract_data(
                data_container[processed_data_type_name], processed_data_type
            )
        elif processed_data_type_settings["collected_info_type"] == "E":
            extracted_data[data_type_mapping_name] = (
                True if data_container[processed_data_type_name] else False
            )
        else:
            return extracted_data

        return extracted_data

    # TODO Improve
    def _extract_data(self, content: dict, data_type_mapping: dict):
        data_type_settings = data_type_mapping["@collected_info"]
        data_type_elements = dict(
            [
                item
                for item in data_type_mapping.items()
                if not item[0].startswith("@collected_info")
            ]
        )

        if data_type_settings["collected_info_type"] == None:
            return None

        if data_type_settings["occurence"] and content is None:
            err_msg = f"Отсутствует значение для обязательного поля, Aliases = {data_type_settings['aliases']}"
            logger.debug(f"{err_msg}")

        if data_type_settings["collected_info_type"] == "V":
            # Проверка на достижение элемента базового типа
            if data_type_settings["type"] == "object" and content is not None:
                if isinstance(content, list):
                    extracted_data = []
                    for item in content:
                        extracted_data.append(
                            {
                                self._data_type_mapping_name(
                                    data_type_elements[key]["@collected_info"]
                                ): self._extract_data(
                                    item.get(key, None),
                                    data_type_elements[key],
                                )
                                for key in data_type_elements
                            }
                        )
                else:
                    extracted_data = {}
                    for key in data_type_elements:
                        extracted_data[
                            self._data_type_mapping_name(
                                data_type_elements[key]["@collected_info"]
                            )
                        ] = self._extract_data(
                            content.get(key, None),
                            data_type_elements[key],
                        )
                return extracted_data
            else:
                if isinstance(content, str):
                    content = self._cast_data_value_to_type(
                        content, data_type_settings["type"]
                    )

                return content

        elif data_type_mapping["collected_info_type"] == "E":
            return False if content is None else True

    def _data_type_mapping_name(self, data_type_settings: dict):
        return data_type_settings["mapping"].split(self.mapping_delimiter)[-1]

    # TODO Improve
    def _cast_data_value_to_type(self, data: str, data_type: str):
        if data_type == "String" or data_type == "LargeBinary":
            return data
        elif data_type == "Integer":
            return int(data)
        elif data_type == "Float":
            return float(data.replace(",", ".").replace(" ", ""))
        elif data_type == "Boolean":
            return data_type.lower() == "true"
        elif data_type == "Date" or data_type == "types.TIMESTAMP(timezone=True)":
            date = dateutil.parser.parse(data, fuzzy=True)
            return date

    @property
    def mapping(self):
        return self._mapping

    @mapping.setter
    def mapping(self, new_mapping):
        self._mapping = new_mapping

    @mapping.deleter
    def mapping(self):
        del self._mapping

    @property
    def mapping_delimiter(self):
        return self._mapping_delimiter

    @mapping_delimiter.setter
    def mapping_delimiter(self, new_delimiter):
        self._mapping_delimiter = str(new_delimiter)

    @mapping_delimiter.deleter
    def mapping_delimiter(self):
        del self._mapping_delimiter

    @property
    def root_prefix(self):
        return self._root_prefix

    @root_prefix.setter
    def root_prefix(self, new_prefix: str):
        self._root_prefix = str(new_prefix)

    @root_prefix.deleter
    def root_prefix(self):
        del self._root_prefix
