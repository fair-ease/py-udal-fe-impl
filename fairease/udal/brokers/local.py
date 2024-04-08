import pandas as pd
import pathlib
from typing import List

from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result


localBrokerQueryNames: List[QueryName] = [
    'urn:fairease.eu:udal:example:weekdays',
    'urn:fairease.eu:udal:example:months',
    'urn:fairease.eu:udal:example:translation'
]


localBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in localBrokerQueryNames }


class LocalBroker(Broker):

    _query_names: List[QueryName] = localBrokerQueryNames

    _queries: dict[QueryName, NamedQueryInfo] = localBrokerQueries

    def __init__(self):
        pass

    @property
    def queryNames(self) -> List[str]:
        return list(LocalBroker._query_names)

    @property
    def queries(self) -> List[NamedQueryInfo]:
        return list(LocalBroker._queries.values())

    @staticmethod
    def _testDataSetPath(filename: str):
        base = pathlib.Path(__file__).parent.parent.parent.parent
        return base.joinpath('test/datasets', filename)

    def _execute_weekdays(self, params: dict):
        data = pd.read_csv(LocalBroker._testDataSetPath('weekdays.csv'))
        if 'lang' in params.keys():
            lang = params['lang']
            if isinstance(lang, str):
                data = data.loc[data['lang'] == lang]
            elif isinstance(lang, list):
                data = data.loc[data['lang'].isin(lang)]
        if 'format' in params.keys():
            format = params['format']
            if format not in ['long', 'short']:
                raise Exception(f'invalid weekday format "{format}"')
            if format == 'long':
                data.drop(columns='short_name', inplace=True)
            else:
                data.drop(columns='name', inplace=True)
        return data

    def _execute_months(self, params: dict):
        data = pd.read_csv(LocalBroker._testDataSetPath('months.csv'))
        if 'lang' in params.keys():
            lang = params['lang']
            if isinstance(lang, str):
                data = data.loc[data['lang'] == lang]
            elif isinstance(lang, list):
                data = data.loc[data['lang'].isin(lang)]
        return data

    def _execute_translation(self):
        # prepare weekday translations
        weekdays = pd.read_csv(LocalBroker._testDataSetPath('weekdays.csv'))
        weekdays = weekdays.filter(items=['lang', 'number', 'name'])
        weekdays = weekdays.pivot(columns='lang', values='name', index='number')
        weekdays = weekdays.rename_axis(None)
        # prepare month translations
        months = pd.read_csv(LocalBroker._testDataSetPath('months.csv'))
        months = months.pivot(columns='lang', values='name', index='number')
        months = months.rename_axis(None)
        # return all translations
        return pd.concat([weekdays, months]).reset_index().drop(columns='index')

    def execute(self, urn: QueryName, params: dict | None = None) -> Result:
        query = LocalBroker._queries[urn]
        queryParams = params or {}
        if urn == 'urn:fairease.eu:udal:example:weekdays':
            return Result(query, self._execute_weekdays(queryParams))
        elif urn == 'urn:fairease.eu:udal:example:months':
            return Result(query, self._execute_months(queryParams))
        elif urn == 'urn:fairease.eu:udal:example:translation':
            return Result(query, self._execute_translation())
        else:
            if urn in QUERY_NAMES:
                raise Exception(f'unsupported query name "{urn}"')
            else:
                raise Exception(f'unknown query name "{urn}"')

