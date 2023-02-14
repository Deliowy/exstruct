import typing

import sqlalchemy
import tenacity
import tenacity.retry
from tenacity import retry

from ..util import util
from .base_client import BaseDBClient

logger = util.getLogger("exstruct.dbclient.sql")


class SQLAlchemyDBClient(BaseDBClient):
    """Handles interactions with SQAlchemy-supported database"""

    def __init__(
        self,
        db_logger: str = "sqlalchemy.engine",
        logging_level: int = util.logging.DEBUG,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(db_logger, logging_level, *args, **kwargs)

    @classmethod
    def get_engine(
        cls,
        *,
        drivername: str,
        username: str,
        password: str,
        host: str,
        port: str,
        database: str,
    ) -> sqlalchemy.engine.Engine:
        """Get engine with given parameters

        Args:
            drivername (str): Type of database and driver to interact with it. Format: {database}+{driver}
            username (str): Database user name
            password (str): Database user password
            host (str): Database host name or IP-adress
            database (str): Database name
            sync (bool, optional): Flag to create syncronous engine. Defaults to False.
            verbose (bool, optional): Flag to print out all SQL commands in stdout and loger-file (can bloat logger-file). Defaults to False.
            port (str, optional): Port used for connection. Defaults to None.

        Returns:
            sqlalchemy.engine.Engine | sqlalchemy.ext.asyncio.AsyncEngine: _description_
        """
        database_url = sqlalchemy.engine.URL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        engine = sqlalchemy.create_engine(database_url)
        return engine

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, new_engine: sqlalchemy.engine.Engine):
        if issubclass(new_engine.__class__, sqlalchemy.engine.Engine):
            self._engine = new_engine
        else:
            err_msg = f"Expected 'sqlalchemy.engine.Engine' or its subclass, not '{new_engine._class__}'"
            raise TypeError(err_msg)

    @engine.deleter
    def engine(self):
        self._engine.dispose()
        del self._engine

    @property
    def logging_level(self):
        return self._logging_level

    @logging_level.setter
    def logging_level(self, new_log_level: int | str):
        self._logging_level = new_log_level
        util.logging.getLogger("sqlalchemy.engine").setLevel(new_log_level)

    def load(self, object: sqlalchemy.orm.DeclarativeMeta, *args, **kwargs):
        return super().load(object, *args, **kwargs)

    @retry(
        stop=tenacity.stop.stop_after_attempt(5),
        after=tenacity.after.after_log(logger=logger, log_level=util.logging.WARNING),
    )
    def _load(self, package: typing.Iterable):
        # Create tables and schema before loading data
        self.init_tables(package[0].metadata)
        with sqlalchemy.orm.Session(self.engine) as session:
            session.add_all(package)
            session.commit()

    def query(self, sql_query: str):
        compiled_sql_query = sqlalchemy.text(sql_query)

        with sqlalchemy.orm.Session(self.engine) as session:
            query_result = session.execute(compiled_sql_query)
            result_content = query_result.fetchall()

        return result_content

    def ping(self):
        try:
            self.engine.connect()
            return True
        except sqlalchemy.exc.OperationalError:
            return False

    def init_tables(
        self, metadata: sqlalchemy.MetaData, engine: sqlalchemy.engine.Engine = None
    ):
        engine = engine if engine else self.engine
        with engine.begin() as conn:
            metadata.create_all(bind=conn)


class AsyncSQLAlchemyDBClient(object):
    """Handles interactions with SQAlchemy-supported database"""

    def __init__(
        self,
        db_logger: str = "sqlalchemy.engine",
        logging_level: int = util.logging.DEBUG,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(db_logger, logging_level, *args, **kwargs)

    @classmethod
    def get_engine(
        cls,
        *,
        drivername: str,
        host: str,
        port: str,
        database: str,
        username: str,
        password: str,
    ) -> sqlalchemy.ext.asyncio.AsyncEngine:
        """Get engine with given parameters

        Args:
            drivername (str): Type of database and driver to interact with it. Format: {database}+{driver}
            host (str): Database host name or IP-adress
            port (str): Port used for connection
            database (str): Database name
            username (str): Database user name
            password (str): Database user password

        Returns:
            sqlalchemy.engine.Engine | sqlalchemy.ext.asyncio.AsyncEngine: _description_
        """
        database_url = sqlalchemy.engine.URL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        engine = sqlalchemy.ext.asyncio.create_async_engine(database_url)
        return engine

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, new_engine: sqlalchemy.ext.asyncio.AsyncEngine):
        if issubclass(new_engine.__class__, sqlalchemy.ext.asyncio.AsyncEngine):
            self._engine = new_engine
        else:
            err_msg = f"Expected 'sqlalchemy.ext.asyncio.AsyncEngine' or its subclass, not '{new_engine.__class__}'"
            raise TypeError(err_msg)

    @engine.deleter
    def engine(self):
        self._engine.dispose()
        del self._engine

    @property
    def logging_level(self):
        return self._logging_level

    @logging_level.setter
    def logging_level(self, new_log_level: int | str):
        self._logging_level = new_log_level
        util.logging.getLogger("sqlalchemy.engine").setLevel(new_log_level)

    @retry(
        stop=tenacity.stop.stop_after_attempt(5),
        after=tenacity.after.after_log(logger=logger, log_level=util.logging.WARNING),
    )
    async def _load(self, package: typing.Iterable):
        await self.init_tables(package[0].metadata)
        async with sqlalchemy.ext.asyncio.AsyncSession(self._engine) as session:
            session.add_all(package)
            await session.commit()

    async def query(self, sql_query: str):
        compiled_sql_query = sqlalchemy.text(sql_query)
        async with sqlalchemy.ext.asyncio.AsyncSession(self.engine) as session:
            query_result = await session.execute(compiled_sql_query)
            result_content = query_result.fetchall()

        return result_content

    async def init_tables(self, metadata: sqlalchemy.MetaData, bind=None):
        engine = bind if bind else self.engine
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
