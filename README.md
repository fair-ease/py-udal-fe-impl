# FAIR-EASE UDAL Implementation Example

See the Uniform Data Access Layer (UDAL) specification for Python at
https://github.com/fair-ease/py-udal-interface and further documentation at
https://lab.fairease.eu/udal/


## Development Setup

Create a virtual environment and install dependencies with
[Poetry](https://python-poetry.org/):

```sh
poetry install --with notebook
```


## Simple Examples

Try the examples with:

```sh
python -m fairease.udal.example
```

This implementation supports three named queries:

- `urn:fairease.eu:udal:example:weekdays` &ndash; days of the week in multiple
  languages;
- `urn:fairease.eu:udal:example:months` &ndash; months of the year in multiple
  languages;
- `urn:fairease.eu:udal:example:translation` &ndash; a translation table
  containing day of the week and month names.

This implementation provides two data brokers:

- `LocalBroker` using the data in `test/datasets`;
- `WikidataBroker` using data from [Wikidata](https://www.wikidata.org/),
  querying it using SPARQL.

`LocalBroker` supports the three named queries, while `WikidataBroker` only
supports the first two.


## Argo Data

This implementation supports one named query:

- `urn:fairease.eu:argo:data` &ndash; Data argo

This implementation provides two data brokers:

- `IddasBroker` using data from [IDDAS](https://fair-ease-iddas.maris.nl) and [Blue-cloud](https://data.blue-cloud.org/) (connection string: `https://fair-ease-iddas.maris.nl`)
- `BeaconBroken` using data from [Beacon](https://beacon.maris.nl/) (connection string: `https://beacon-argo.maris.nl`)
