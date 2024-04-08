import typing
from typing import List, Literal


class NamedValue():
    """A field with a name in a parameter list or a result column."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def as_dict(self) -> dict:
        return {
            'name': self._name,
        }


class TypedValue(NamedValue):
    """A named field and a description of its type."""

    def __init__(self, name: str, type: str):
        super().__init__(name)
        self._type = type

    @property
    def type(self) -> str:
        return self._type

    def as_dict(self) -> dict:
        return {
            'name': self._name,
            'type': self._type,
        }


class NamedQueryInfo():
    """Information about a named query, namely its name, parameters, and
    fields."""

    def __init__(self,
            name: str,
            params: List[NamedValue],
            fields: List[NamedValue]):
        self._name = name
        self._params = params
        self._fields = fields

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> List[NamedValue]:
        return self._params

    @property
    def fields(self) -> List[NamedValue]:
        return self._fields

    def as_dict(self) -> dict:
        return {
            'name': self._name,
            'params': self._params,
            'fields': self._fields,
        }


QueryName = Literal[
    'urn:fairease.eu:udal:example:weekdays',
    'urn:fairease.eu:udal:example:months',
    'urn:fairease.eu:udal:example:translation'
    ]


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)


QUERY_REGISTRY : dict[QueryName, NamedQueryInfo] = {
    'urn:fairease.eu:udal:example:weekdays': NamedQueryInfo(
            'urn:fairease.eu:udal:example:weekdays',
            [
                TypedValue('lang', 'str|List[str]'),
                TypedValue('format', '"long"|"short"'),
            ],
            [],
        ),
    'urn:fairease.eu:udal:example:months': NamedQueryInfo(
            'urn:fairease.eu:udal:example:months',
            [
                TypedValue('lang', 'str|List[str]'),
            ],
            [],
        ),
    'urn:fairease.eu:udal:example:translation': NamedQueryInfo(
            'urn:fairease.eu:udal:example:translation',
            [],
            [],
        ),
}
