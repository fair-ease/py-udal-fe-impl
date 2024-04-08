import pandas
from typing import Any

import udal.specification as udal

from .namedqueries import NamedQueryInfo


class Result(udal.Result):
    """Result from executing an UDAL query."""

    Type = pandas.DataFrame

    def __init__(self, query: NamedQueryInfo, data: Any, metadata: dict = {}):
        self._query = query
        self._data = data
        self._metadata = metadata

    @property
    def query(self):
        """Information about the query that generated the data in this
        result."""
        return self._query

    @property
    def metadata(self):
        """Metadata associated with the result data."""
        return self._metadata

    def data(self, type: type[Type] | None = None) -> Type:
        """The data of the result."""
        if type is None or type is pandas.DataFrame:
            return self._data
        raise Exception(f'type "{type}" not supported')
