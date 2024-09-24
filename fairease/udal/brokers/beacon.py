import datetime
from pathlib import Path
import tempfile
from typing import List
import requests
import os
import xarray as xr
import warnings


from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result
from ..config import Config

beaconBrokerQueryName: List[QueryName] = [
    'urn:fairease.eu:argo:data'
]

beaconBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in beaconBrokerQueryName }


class BeaconBroker(Broker):

    _config: Config

    _queryNames: List[QueryName] = beaconBrokerQueryName

    _queries: dict[QueryName, NamedQueryInfo] = beaconBrokerQueries

    @property
    def queryNames(self) -> List[str]:
        return list(BeaconBroker._queryNames)

    @property
    def queries(self) -> List[NamedQueryInfo]:
        return list(BeaconBroker._queries.values())

    def __init__(self, config: Config):
        
        self._config = config
        if not self._config or not self._config.beacon_token:
            raise Exception('Please provide a token')
        self.token = self._config.beacon_token

    def _execute_argo(self, params: dict):
        json_params = {
            "query_parameters": [
                {"column_name": "JULD", "alias": "TIME"},
                {"column_name": "PRES", "alias": "Depth [meter]"},
                {"column_name": "LATITUDE", "alias": "Latitude"},
                {"column_name": "LONGITUDE", "alias": "Longitude"},
            ],
            "filters": [],
            "output": {"format": "netcdf"}
        }

        # Validate input parameters
        if not any(k in params for k in ('parameter', 'startTime', 'endTime')):
            raise ValueError('Please provide at least one of the following parameters: parameter, startTime, endTime')

        # Add parameters if they exist
        if 'parameter' in params:
            if 'temperature' in params['parameter']:
                json_params['query_parameters'].append({"column_name": "TEMP", "alias": "Temperature [degree_Celsius]"})
            if 'salinity' in params['parameter']:
                json_params['query_parameters'].append({"column_name": "PSAL", "alias": "Salinity [PSU]"})
            if 'pressure' in params['parameter']:
                json_params['query_parameters'].append({"column_name": "PRES", "alias": "Pressure [dbar]"})

        # Process date range
        if 'startTime' in params and 'endTime' in params:
            date_ref = datetime.date(1950, 1, 1)
            start_date = datetime.datetime.strptime(params['startTime'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(params['endTime'], '%Y-%m-%d').date()
            min_temporal = (start_date - date_ref).days
            max_temporal = (end_date - date_ref).days

            if max_temporal < min_temporal:
                raise ValueError('The start date must be before the end date. Please update your input fields above and run the notebook again.')

            if max_temporal - min_temporal > 31:
                raise ValueError(f'The maximum time range is 31 days. Please update your input fields above and run the notebook again. The current range is {max_temporal - min_temporal} days.')

            json_params['filters'].append({"for_query_parameter": "TIME", "min": min_temporal, "max": max_temporal})

        # Latitude and longitude
        if 'latitude' in params:
            min = params['latitude'] - 0.5
            max = params['latitude'] + 0.5
            json_params['filters'].append({"for_query_parameter": "Latitude", "min": min, "max": max})
        if 'longitude' in params:
            min = params['longitude'] - 0.5
            max = params['longitude'] + 0.5
            json_params['filters'].append({"for_query_parameter": "Longitude", "min": min, "max": max})

        # bounding box
        if 'bounding_box' in params:
            params.pop('bounding_box')
            warnings.warn('Bounding box is not implemented')

        # Create filename
        params_str = "_".join(f"[{','.join(map(str, params[key])) if isinstance(params[key], list) else params[key]}]" for key in params.keys())
        file_name = f"beacon_argo_{params_str}.nc"

        def request_data(json_params, file_name):
            response = requests.post(
                'https://beacon-argo.maris.nl/api/query',
                json=json_params,
                headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'},
                stream=True
            )
            response.raise_for_status()
            with open(dir.joinpath(file_name), 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)


        if self._config.cache_dir is None:
            with tempfile.TemporaryDirectory(prefix='fairease-udal-') as temp_dir:
                dir = Path(temp_dir).joinpath('data')
                try:
                    os.makedirs(dir)
                except:
                    pass

                try:

                    if not dir.joinpath(file_name).exists():
                        request_data(json_params, file_name)
                        
                    data = xr.open_dataset(dir.joinpath(file_name), engine='netcdf4')
                    data.close()

                    return data
                
                except requests.RequestException as e:
                    raise Exception(f'Error: {e}')
                except Exception as e:
                    raise Exception(f'Error: {e}')
        else:
            dir = Path(self._config.cache_dir).joinpath('data')
            try:
                os.makedirs(dir)
            except:
                pass

            try:
                
                if not dir.joinpath(file_name).exists():
                    request_data(json_params, file_name)
                
                if dir.joinpath(file_name).stat().st_size == 0:
                    raise Exception('No data found for the given parameters')

                data = xr.open_dataset(dir.joinpath(file_name), engine='netcdf4')
                data.close()

                return data
                
            except requests.RequestException as e:
                raise Exception(f'Error: {e}')
            except Exception as e:
                raise Exception(f'Error: {e}')

    def execute(self, urn: QueryName, params: dict|None = None)-> Result: 
        query = BeaconBroker._queries[urn]
        queryParams = params or {}

        if urn == 'urn:fairease.eu:argo:data':
            return Result(query, self._execute_argo(queryParams))
        else:
            if urn in QUERY_NAMES:
                raise Exception(f'unsupported query name "{urn}"')
            else :
                raise Exception(f'unknown query name "{urn}"')
