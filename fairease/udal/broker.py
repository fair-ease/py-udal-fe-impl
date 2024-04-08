from abc import ABC, abstractmethod

from .namedqueries import QueryName
from .result import Result


class Broker(ABC):

    @abstractmethod
    def execute(self, urn: QueryName, params: dict | None = None) -> Result:
        pass
