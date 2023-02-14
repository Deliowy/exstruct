import typing

import more_itertools
import pymongo
import tenacity
import tenacity.retry
from tenacity import retry

from ..util import util
from .base_client import BaseDBClient

logger = util.getLogger("exstruct.dbclient.no_sql")


class MongoDBClient(BaseDBClient):
    """Handles interaction with MongoDB database"""

    def __init__(
        self,
        sync: bool = False,
        logging_level: int = util.logging.INFO,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(sync, logging_level, *args, **kwargs)

    @classmethod
    def get_engine(cls, sync, verbose, *args, **kwargs):
        drivername = kwargs.pop("drivername", "mongodb")

        host = kwargs.pop("host", "localhost")
        port = kwargs.pop("port", 27017)

        tz_aware = kwargs.pop("tz_aware", False)

        username = kwargs.pop("username", "user")
        password = kwargs.pop("password", "")
        authSource = kwargs.pop("authSource", "admin")
        authMechanism = kwargs.pop("authMechanism", "SCRAM-SHA-256")

        connection_string = f"{drivername}://{host}/"
        engine = pymongo.MongoClient(
            connection_string,
            port=port,
            tz_aware=tz_aware,
            username=username,
            password=password,
            authSource=authSource,
            authMechanism=authMechanism,
        )
        return engine

    def _load(self, batch: typing.Iterable, *args, **kwargs):
        database = self.engine[kwargs.pop("database")][
            kwargs.pop("collection", "public")
        ]
        result_ids = database.insert_many(batch)
        return result_ids

    def query(self, query: str):
        return super().query(query)