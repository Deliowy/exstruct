import abc
import typing

import more_itertools

from ..util import _util

logger = _util.getLogger("exstruct.dbclient.base_client")


class BaseDBClient(object):
    """Prototype class for intercations with databases"""

    def __init__(
        self,
        db_logger: str = None,
        logging_level: int = _util.logging.DEBUG,
        *args,
        **kwargs,
    ) -> None:
        self._verbose = kwargs.pop("verbose", False)
        self._db_logger = db_logger
        self.logging_level = logging_level
        self._engine = self.get_engine(**kwargs)
        self._engine_kwargs = kwargs

        if self.verbose:
            self.logging_level = _util.logging.INFO

    def batch_load(
        self, package: typing.Iterable, batch_size: int = None, *args, **kwargs
    ):
        if batch_size:
            packages_to_process = more_itertools.batched(package, batch_size)
        else:
            packages_to_process = [package]

        results = []

        for batch in packages_to_process:
            results.extend(self._load(batch, *args, **kwargs))

        return results

    def load(self, object, *args, **kwargs):
        result = self._load([object], *args, **kwargs)
        return result

    @abc.abstractmethod
    def _load(self, batch: typing.Iterable, *args, **kwargs):
        pass

    @abc.abstractmethod
    def query(self, query: str):
        pass

    @abc.abstractmethod
    def ping(self):
        pass

    @classmethod
    @abc.abstractmethod
    def get_engine(cls, *args, **kwargs):
        pass

    @property
    def verbose(self):
        return self._verbose

    @property
    def logging_level(self):
        return self._logging_level

    @logging_level.setter
    def logging_level(self, new_logging_level: int | str):
        self._logging_level = new_logging_level
        _util.logging.getLogger(self.db_logger).setLevel(new_logging_level)

    @property
    def db_logger(self):
        return self._db_logger

    @property
    def engine(self):
        return self._engine

    @engine.setter
    @abc.abstractmethod
    def engine(self, engine):
        self._engine = engine

    @engine.deleter
    @abc.abstractmethod
    def engine(self):
        del self._engine
