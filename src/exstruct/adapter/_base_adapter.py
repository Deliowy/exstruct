import abc

from .. import util


class BaseAdapter(abc.ABC):
    """Prototype class for adaptation of data to python objects. All adapters must be iherited from it"""

    @abc.abstractmethod
    def transform(self, data):
        return data
