from typing import Literal

import udal.specification as udal

from .brokers.local import LocalBroker
from .brokers.wikidata import WikidataBroker
from .brokers.beacon import BeaconBroker
from .brokers.iddas import IDDASBroker
from .namedqueries import QUERY_NAMES, QueryName
from .result import Result

Connection = Literal['https://www.wikidata.org/', 'https://beacon-argo.maris.nl', 'https://fair-ease-iddas.maris.nl']

class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, connectionString: Connection | None = None, config: udal.Config = udal.Config()):
        self._config = config
        if connectionString is None:
            self._broker = LocalBroker()
        elif connectionString == 'https://www.wikidata.org/':
            self._broker = WikidataBroker()
        elif connectionString == 'https://beacon-argo.maris.nl':
            self._broker = BeaconBroker(self._config)
        elif connectionString == 'https://fair-ease-iddas.maris.nl':
            self._broker = IDDASBroker(self._config)

    def execute(self, name: str, params: dict|None = None) -> Result:
        if name in QUERY_NAMES:
            return self._broker.execute(name, params)
        else:
            raise Exception(f'query {name} not supported')

    @property
    def queries(self) -> dict[str, udal.NamedQueryInfo]:
        return self._broker.queries
