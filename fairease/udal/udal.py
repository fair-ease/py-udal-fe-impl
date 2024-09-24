from typing import Literal

import udal.specification as udal

from .brokers.local import LocalBroker
from .brokers.wikidata import WikidataBroker
from .brokers.beacon import BeaconBroker
from .brokers.iddas import IDDASBroker
from .namedqueries import QueryName
from .result import Result
from .config import Config

Connection = Literal['https://www.wikidata.org/', 'https://beacon-argo.maris.nl', 'https://fair-ease-iddas.maris.nl']

class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, connectionString: Connection | None = None, config: Config | None = None):
        self._config = config or Config()
        if connectionString is None:
            self._broker = LocalBroker()
        elif connectionString == 'https://www.wikidata.org/':
            self._broker = WikidataBroker()
        elif connectionString == 'https://beacon-argo.maris.nl':
            self._broker = BeaconBroker(self._config)
        elif connectionString == 'https://fair-ease-iddas.maris.nl':
            self._broker = IDDASBroker(self._config)
    def execute(self, urn: QueryName, params: dict|None = None) -> Result:
        return self._broker.execute(urn, params)

    @property
    def query_names(self):
        return self._broker.queryNames

    @property
    def queries(self):
        return self._broker.queries
