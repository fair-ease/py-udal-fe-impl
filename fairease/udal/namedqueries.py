import typing
from typing import List, Literal

from udal.specification import NamedQueryInfo
import udal.specification as udal


QueryName = Literal[
    'urn:fairease.eu:udal:example:weekdays',
    'urn:fairease.eu:udal:example:months',
    'urn:fairease.eu:udal:example:translation',
    'urn:fairease.eu:argo:data'
    ]


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)


QUERY_REGISTRY : dict[QueryName, NamedQueryInfo] = {
    'urn:fairease.eu:udal:example:weekdays': NamedQueryInfo(
            'urn:fairease.eu:udal:example:weekdays',
            {
                'lang': ['str', udal.tlist('str')],
                'format': [udal.tliteral("long"), udal.tliteral("short")],
            },
        ),
    'urn:fairease.eu:udal:example:months': NamedQueryInfo(
            'urn:fairease.eu:udal:example:months',
            {
                'lang': ['str', udal.tlist('str')],
            },
        ),
    'urn:fairease.eu:udal:example:translation': NamedQueryInfo(
            'urn:fairease.eu:udal:example:translation',
            {},
        ),
    'urn:fairease.eu:argo:data': NamedQueryInfo(
            'urn:fairease.eu:argo:data',
            {
                'parameter': ['str', udal.tlist('str')],
                'startTime': 'str',
                'endTime': 'str',
                'bounding_box': udal.tdict('number'),
                'longitude': 'number',
                'latitude': 'number',
            },
        ),
}
