import concurrent.futures
import json
import pathlib
import tempfile
import typing
from concurrent.futures import ThreadPoolExecutor

import dateutil
import dateutil.parser
import h5py
import numpy

from ..util import _util

logger = _util.getLogger("exstruct.extractor.extractor")

DEFAULT_BATCH_SIZE = 10_000


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
        self._batch_size = kwargs.pop("batch_size", DEFAULT_BATCH_SIZE)

    def extract_from_file(self, filename: str):
        extracted_data_file = tempfile.TemporaryFile(
            dir=pathlib.Path(filename).parent, delete=False, suffix=".hdf5"
        )
        extracted_data_hdf5 = h5py.File(extracted_data_file, "r+")
        extracted_data_dset = extracted_data_hdf5.create_dataset(
            "extracted_data",
            dtype=h5py.string_dtype("utf-8"),
            shape=(0,),
            maxshape=(None,),
            compression="gzip",
            chunks=(self._batch_size,),
            shuffle=True,
        )

        file = h5py.File(filename)
        for key in file.keys():
            dset = file[key]
            batch = []
            processed_data = 0
            with ThreadPoolExecutor(max_workers=100) as executor:
                futures_to_extracted_data = [
                    executor.submit(self.extract, pre_extracted_data)
                    for pre_extracted_data in map(json.loads, dset)
                ]

            for future in concurrent.futures.as_completed(futures_to_extracted_data):
                extracted_data = future.result()
                batch.append(
                    json.dumps(extracted_data, ensure_ascii=False, default=str)
                )
                if len(batch) > self._batch_size:
                    extracted_data_dset.resize((processed_data + self._batch_size,))
                    extracted_data_dset[processed_data:] = numpy.asarray(
                        batch[: self._batch_size + 1]
                    )
                    processed_data += self._batch_size + 1
                    extracted_data_dset.flush()
                    del batch[: self._batch_size + 1]

            if batch:
                extracted_data_dset.resize((processed_data + len(batch),))
                extracted_data_dset[processed_data:] = numpy.asarray(batch)
                extracted_data_dset.flush()
                processed_data += len(batch)
                del batch

        file.close()
        extracted_data_hdf5.flush()
        extracted_data_hdf5.close()
        return extracted_data_file

    def extract(self, content: dict | typing.Iterable[dict]):
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
            try:
                date = dateutil.parser.parse(data)
            except dateutil.parser.ParserError:
                date = dateutil.parser.isoparse(data)
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
