import io
from pathlib import Path
import zipfile
from SPARQLWrapper import SPARQLWrapper, JSON 
import numpy as np
import requests
from requests.exceptions import RequestException
import os
import xarray as xr
import time
import json
from typing import List, Dict, Tuple, Union
import tempfile

import swagger_client
from swagger_client.rest import ApiException

from ..config import Config

from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result

iddasBrokerQueryName: List[QueryName] = [
    'urn:fairease.eu:argo:data',
]

iddasBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in iddasBrokerQueryName }


class IDDASBroker(Broker):

    _queryNames: List[QueryName] = iddasBrokerQueryName
    
    _config: Config


    _queries: dict[QueryName, NamedQueryInfo] = iddasBrokerQueries

    @property
    def queryNames(self) -> List[str]:
        return list(IDDASBroker._queryNames)

    @property
    def queries(self) -> List[NamedQueryInfo]:
        return list(IDDASBroker._queries.values())

    def __init__(self, config: Config):
        self.dict_params = {
                'temperature': 'TEMP',
                'salinity': 'PSAL',
                'pressure': 'PRES'
            }
        self._config = config
        if not self._config or not self._config.blue_cloud_token:
            raise ValueError('Please provide a token')
        self.token = self._config.blue_cloud_token

        self.catalog = None
        self.base_url = 'https://data.blue-cloud.org/api'
        self.sparql_url = 'https://fair-ease-iddas.maris.nl/sparql/query'
        self.headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}
    
    def _extract_query_param(self, query: str, param: str) -> str:
        """Extracts the value of a query parameter from a query string."""
        try:
            return query.split(f"{param}=")[1].split("&")[0]
        except IndexError:
            raise ValueError(f"Query parameter '{param}' not found in the query string.")
        
    def _build_sparql_filter(self, params: Dict[str, Union[str, float, int]]) -> str:
        """Builds SPARQL filter string from parameters."""
        sparql_filter = []

        # Validate input parameters
        if not any(k in params for k in ('parameter', 'startTime', 'endTime')):
            raise ValueError('Please provide at least one of the following parameters: parameter, startTime, endTime')
     
        if 'parameter' in params:
            param_value = params['parameter']
            
            if isinstance(param_value, str):
                if param_value not in self.dict_params:
                    raise ValueError(f"Parameter '{param_value}' not supported. Please select one of the following parameters: {', '.join(self.dict_params.keys())}")
            
            elif isinstance(param_value, list):
                unsupported_params = [p for p in param_value if p not in self.dict_params]
                if unsupported_params:
                    raise ValueError(f"Parameters '{', '.join(unsupported_params)}' not supported. Please select one of the following parameters: {', '.join(self.dict_params.keys())}")
            
            else:
                raise TypeError(f"Expected parameter to be a string or a list of strings, but got {type(param_value).__name__}.")

        if 'startTime' in params:
            sparql_filter.append(f"FILTER(BOUND(?startDate) && ?startDate >= '{params['startTime']}'^^xsd:date) .")
        if 'endTime' in params:
            sparql_filter.append(f"FILTER(BOUND(?endDate) && ?endDate <= '{params['endTime']}'^^xsd:date) .")
        if 'latitude' in params and 'longitude' in params:
            sparql_filter.append(f"FILTER(BOUND(?bbox) && ?bbox = 'POINT ({params['latitude']} {params['longitude']})'^^geo:wktLiteral) .")
        elif 'latitude' in params or 'longitude' in params:
            raise ValueError("Both latitude and longitude must be provided.")
        if 'bounding_box' in params:
            if not isinstance(params['bounding_box'], dict):
                raise ValueError("Bounding box must be a dictionary with keys 'north', 'east', 'south', 'west'.")
            if not all(k in params['bounding_box'] for k in ['north', 'east', 'south', 'west']):
                raise ValueError("Bounding box must be a dictionary with keys 'north', 'east', 'south', 'west'.")
            sparql_filter.append(
                f"FILTER(BOUND(?bbox) && ?bbox = 'POLYGON (({params['bounding_box']['north']} {params['bounding_box']['east']}, "
                f"{params['bounding_box']['north']} {params['bounding_box']['west']}, {params['bounding_box']['south']} {params['bounding_box']['west']}, "
                f"{params['bounding_box']['south']} {params['bounding_box']['east']}, {params['bounding_box']['north']} {params['bounding_box']['east']}))'^^geo:wktLiteral) ."
            )

        return " ".join(sparql_filter)

    def _get_list_distribution(self, results: dict) -> List[str]:
        """Extracts the list of distributions from the SPARQL query results."""
        if not results['results']['bindings']:
            raise Exception('No data has been found for your query, please update your input fields and try again.')

        for result in results['results']['bindings']:
            if 'netcdf' not in result['mediaType']['value']:
                raise Exception(f"Media type '{result['mediaType']['value']}' is not supported. Please select a NetCDF media type.")

        list_distribution = [
            result['distribution']['value'].replace("#distribution", "")
            for result in results['results']['bindings']
        ]

        if not list_distribution:
            raise Exception('No data has been found for your query, please update your input fields and try again.')

        return list_distribution

    def _prepare_file_names(self, file_name: str, list_distribution: List[str]) -> List[str]:
        """Prepares platform cycle and file names from distributions."""
        file_name = str(file_name)
        if self.catalog == "argo":
            list_plataform_cycle = [
                f"platform-{self._extract_query_param(dist, 'platform')}_cycle-{self._extract_query_param(dist, 'cycle')}"
                for dist in list_distribution
            ]
            list_files = [
                f"{file_name.split('.nc')[0]}_{plataform_cycle}.nc"
                for plataform_cycle in list_plataform_cycle
            ]
            return list_files

        raise ValueError("Catalog not supported.")

    def _remove_existing_files(self, list_files: List[str], list_distribution: List[str]):
        """Removes existing files from the distribution list."""
        for file in list_files:
            if os.path.exists(file):
                for distribution in list_distribution:
                    if self.catalog == 'argo':
                        platform_cycle = (
                            f'platform-{self._extract_query_param(distribution, "platform")}_cycle-'
                            f'{self._extract_query_param(distribution, "cycle")}'
                        )
                        if platform_cycle in file:
                            list_distribution.remove(distribution)
                    else : 
                        raise ValueError("Catalog not supported.")
                    
        return list_distribution

    def _create_folder_name(self, params: Dict[str, Union[str, float, int]]) -> str:
        """Creates a folder name based on parameters."""
        folder_name_filter = "_".join(
            f"{','.join(map(str, params[key])) if isinstance(params[key], list) else params[key]}"
            for key in sorted(params.keys())
        )
        return folder_name_filter.replace(" ", "_").replace(":", "_").replace("-", "_").replace(",", "_")

    def _execute_argo(self, params: dict):
        """Executes the ARGO data retrieval process."""
        self.catalog = "argo"
        sparql_filter = self._build_sparql_filter(params)
        folder_name_filter = self._create_folder_name(params)
        file_name = f"iddas_{self.catalog}.nc"
        query = f"""
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX dc: <http://purl.org/dc/terms/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?distribution ?mediaType ?downloadURL WHERE {{
            ?dataset a dcat:Dataset ;
                dc:title ?_title ;
                dc:description ?description .
            OPTIONAL {{
                ?dataset dc:temporal [
                    a dc:PeriodOfTime ;
                    dcat:startDate ?startDate ;
                    dcat:endDate ?endDate
                ] .
            }}
            OPTIONAL {{
                ?dataset dc:spatial [
                    a dc:Location ;
                    dcat:bbox ?bbox
                ] .
            }}
            OPTIONAL {{
                ?dataset schema:variableMeasured [
                    a schema:PropertyValue ;
                    schema:name ?parameterName
                ] .
            }}
            OPTIONAL {{
                ?dataset prov:used ?used .
            }}
            OPTIONAL {{
                ?catalog a dcat:Catalog ;
                dcat:dataset ?dataset .
            }}

            ?dataset dcat:distribution ?distribution .
            ?distribution dcat:downloadURL ?downloadURL .
            ?distribution dcat:mediaType ?mediaType .

            FILTER (BOUND(?catalog) && STRSTARTS(STR(?catalog), 'https://data.blue-cloud.org/search/dcat/argo') ) . 
            {sparql_filter}
        }} GROUP BY ?dataset ?distribution ?mediaType ?downloadURL"""
        
        sparql = SPARQLWrapper(self.sparql_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        def download_and_process_files(dir: Path, file_name: str, list_distribution: List[str], results: dict):
            list_distribution = self._get_list_distribution(results)

            download_urls = []

            for result in results['results']['bindings']:
                if result['downloadURL']['value']:
                    download_urls.append(result['downloadURL']['value'])

            for i, download_url in enumerate(download_urls):
                list_plataform_cycle = [
                    f"platform-{self._extract_query_param(dist, 'platform')}_cycle-{self._extract_query_param(dist, 'cycle')}"
                    for dist in list_distribution
                ]
                file_name_temp = f"{file_name.split('.nc')[0]}_{list_plataform_cycle[i]}.nc"
                
                header = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/zip'}
                response = requests.get(download_url, headers=header)
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    for file in z.namelist():
                        if file.endswith('prof.nc'):
                            z.extract(file, dir)
                            os.rename(dir.joinpath(file), dir.joinpath(file_name_temp))

        def do_processing(dataset: xr.Dataset, params: dict):
            dataset.close()
            try:
                list_data_vars = ['JULD', 'LATITUDE', 'LONGITUDE']

                parameter = params['parameter']
                if isinstance(parameter, str):
                    list_data_vars.append(self.dict_params[parameter])
                elif isinstance(parameter, list):
                    for param in parameter:
                        list_data_vars.append(self.dict_params[param])

                dataset = dataset[list_data_vars]

                return dataset
            except KeyError:
                return None
            except Exception as e:
                raise Exception(f'Error: {e}')

        def process_and_return_datasets(dir: Path, params: dict):
            ds = []
            for file in dir.iterdir():
                dataset = do_processing(xr.open_dataset(file), params)
                if dataset is not None:
                    ds.append(dataset)
    
            return ds
        if self._config.cache_dir is None:
            with tempfile.TemporaryDirectory(prefix='fairease-udal-') as temp_dir:
                dir = Path(temp_dir).joinpath(folder_name_filter)
                try:
                    os.mkdir(dir)
                except FileExistsError:
                    pass

                list_distribution = self._get_list_distribution(results)

                download_and_process_files(dir, file_name, list_distribution, results)
                return process_and_return_datasets(dir, params)

        else:
            dir = Path(self._config.cache_dir).joinpath(folder_name_filter)
            try:
                os.mkdir(dir)
            except FileExistsError:
                pass

            list_distribution = self._get_list_distribution(results)
            list_files = self._prepare_file_names(dir.joinpath(file_name), list_distribution)
            list_distribution = self._remove_existing_files(list_files, list_distribution)

            if list_distribution:
                download_and_process_files(dir, file_name, list_distribution, results)

            return process_and_return_datasets(dir, params)

    def execute(self, urn: QueryName, params: dict|None = None) -> Result:
        query = IDDASBroker._queries[urn]
        queryParams = params or {}

        if urn == 'urn:fairease.eu:argo:data':
            return Result(query, self._execute_argo(queryParams))
        else:
            if urn in QUERY_NAMES:
                raise Exception(f'unsupported query name "{urn}"')
            else :
                raise Exception(f'unknown query name "{urn}"')

    