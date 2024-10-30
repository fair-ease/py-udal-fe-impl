from abc import ABC, abstractmethod

from udal.specification import NamedQueryInfo

from .namedqueries import QueryName
from .result import Result


class Broker(ABC):

    @property
    @abstractmethod
    def queries(self) -> dict[str, NamedQueryInfo]:
        """Information about the queries supported by the current
        implementation."""
        pass

    @abstractmethod
    def execute(self, name: QueryName, params: dict | None = None) -> Result:
        pass
