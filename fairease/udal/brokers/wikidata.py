import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import typing
from typing import List

from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result


wikidataBrokerQueryNames: List[QueryName] = [
    'urn:fairease.eu:udal:example:weekdays',
    'urn:fairease.eu:udal:example:months',
]


wikidataBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in wikidataBrokerQueryNames }


class WikidataBroker(Broker):

    _WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql'

    _queryNames: List[QueryName] = wikidataBrokerQueryNames

    _queries: dict[QueryName, NamedQueryInfo] = wikidataBrokerQueries

    @property
    def queryNames(self) -> List[str]:
        return list(WikidataBroker._queryNames)

    @property
    def queries(self) -> List[NamedQueryInfo]:
        return list(WikidataBroker._queries.values())

    @staticmethod
    def filterQueryForLangs(var: str, langs: List[str]) -> str:
        def filterExpr(lang: str) -> str:
            return f'(langMatches(lang(?{var}), "{lang}"))'
        return f'FILTER (' + ' || '.join(list(map(filterExpr, langs))) + ')'

    def _execute_weekdays(self, params: dict):
        sparqlFilter = ''
        if 'lang' in params.keys():
            lang = params['lang']
            if isinstance(lang, str):
                sparqlFilter = WikidataBroker.filterQueryForLangs('dayOfWeekLabel', [lang])
            elif isinstance(lang, list):
                sparqlFilter = WikidataBroker.filterQueryForLangs('dayOfWeekLabel', lang)
        q = """
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX p: <http://www.wikidata.org/prop/>
            PREFIX ps: <http://www.wikidata.org/prop/statement/>
            PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT DISTINCT ?dayOfWeek ?dayOfWeekLabel ?dayOfWeekOrdinal (lang(?dayOfWeekLabel) as ?dayOfWeekLang)
            WHERE {
                # instance of day of week
                ?dayOfWeek wdt:P31 wd:Q41825.
                # sub class of day
                ?dayOfWeek wdt:P279 wd:Q573.
                # day of week ordinal
                ?dayOfWeek p:P1545 ?x.
                ?x ps:P1545 ?dayOfWeekOrdinal.
                ?x pq:P1013 wd:Q50101.
                # day of week label
                ?dayOfWeek rdfs:label ?dayOfWeekLabel.
                # language filter (if any)
                """ + sparqlFilter + """
            }
        """
        sparql = SPARQLWrapper(WikidataBroker._WIKIDATA_SPARQL_ENDPOINT)
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        sparqlResults = typing.cast(dict, sparql.queryAndConvert())
        data = pd.json_normalize(sparqlResults['results']['bindings'])
        data = data.filter(items=[
            'dayOfWeekOrdinal.value',
            'dayOfWeekLabel.value',
            'dayOfWeekLang.value',
            ])
        data = data.rename(columns={
            'dayOfWeekOrdinal.value': 'number',
            'dayOfWeekLabel.value': 'name',
            'dayOfWeekLang.value': 'lang',
            })
        return data

    def _execute_months(self, params: dict):
        sparqlFilter = ''
        if 'lang' in params.keys():
            lang = params['lang']
            if isinstance(lang, str):
                sparqlFilter = WikidataBroker.filterQueryForLangs('monthLabel', [lang])
            elif isinstance(lang, list):
                sparqlFilter = WikidataBroker.filterQueryForLangs('monthLabel', lang)
        q = """
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX p: <http://www.wikidata.org/prop/>
            PREFIX ps: <http://www.wikidata.org/prop/statement/>
            PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT DISTINCT ?month ?monthLabel ?monthOrdinal (lang(?monthLabel) as ?monthLang)
            WHERE {
                # instance of calendar month
                ?month wdt:P31 wd:Q47018901.
                # sub class of month of the Gregorian calendar
                ?month wdt:P279 wd:Q18602249.
                # month ordinal
                ?month p:P1545 ?x.
                ?x ps:P1545 ?monthOrdinal.
                ?x pq:P1013 wd:Q50101.
                # day of week label
                ?month rdfs:label ?monthLabel.
                """ + sparqlFilter + """
            }
        """
        sparql = SPARQLWrapper(WikidataBroker._WIKIDATA_SPARQL_ENDPOINT)
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        sparqlResults = typing.cast(dict, sparql.queryAndConvert())
        data = pd.json_normalize(sparqlResults['results']['bindings'])
        data = data.filter(items=[
            'monthOrdinal.value',
            'monthLabel.value',
            'monthLang.value',
            ])
        data = data.rename(columns={
            'monthOrdinal.value': 'number',
            'monthLabel.value': 'name',
            'monthLang.value': 'lang',
            })
        return data

    def execute(self, urn: QueryName, params: dict|None = None) -> Result:
        query = WikidataBroker._queries[urn]
        queryParams = params or {}
        if urn == 'urn:fairease.eu:udal:example:weekdays':
            return Result(query, self._execute_weekdays(queryParams))
        elif urn == 'urn:fairease.eu:udal:example:months':
            return Result(query, self._execute_months(queryParams))
        else:
            if urn in QUERY_NAMES:
                raise Exception(f'unsupported query name "{urn}"')
            else:
                raise Exception(f'unknown query name "{urn}"')
