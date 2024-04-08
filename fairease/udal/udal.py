from typing import Literal

import udal.specification as udal

from .brokers.local import LocalBroker
from .brokers.wikidata import WikidataBroker
from .namedqueries import QueryName
from .result import Result


class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, connectionString: Literal['https://www.wikidata.org/'] | None = None):
        if connectionString is None:
            self._broker = LocalBroker()
        elif connectionString == 'https://www.wikidata.org/':
            self._broker = WikidataBroker()

    def execute(self, urn: QueryName, params: dict|None = None) -> Result:
        return self._broker.execute(urn, params)

    @property
    def query_names(self):
        return self._broker.queryNames

    @property
    def queries(self):
        return self._broker.queries
