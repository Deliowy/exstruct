import abc

from ..util import _util

PARSER_BATCH_SIZE = 10000


class BaseParser(abc.ABC):
    """Prototype class, decsribing common methods and parameters of all parsers"""

    def __init__(self, source: str, response_type: str, **kwargs) -> None:
        """
        Args:
            source (str): path to data-source
            response_type (str): expected response type

        kwargs:
            user (str): username
            password (str): password
        """
        self._source = source
        self._response_type = response_type
        self._user = kwargs.pop("user", None)
        self._password = kwargs.pop("password", None)
        self.logger = _util.getLogger(f"{self.__module__}.{self.__class__.__name__}")

    @abc.abstractmethod
    def parse(self):
        """Start parsing process with current parameters"""
        pass

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, new_source):
        self._source = new_source

    @source.deleter
    def source(self):
        del self._source

    @property
    def response_type(self):
        return self._response_type

    @response_type.setter
    def response_type(self, new_type: str):
        self._response_type = str(new_type)

    @response_type.deleter
    def response_type(self):
        del self._response_type

    @property
    def auth_params(self):
        return {
            "user": self._user,
            "password": self._password,
        }

    @auth_params.setter
    def auth_params(self, **auth_params):
        for name, value in auth_params:
            self.__setattr__(f"_{name}", value)

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, new_user: str):
        self._user = str(new_user)

    @user.deleter
    def user(self):
        del self._user

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, new_password: str):
        self._password = str(new_password)

    @password.deleter
    def password(self):
        del self._password
