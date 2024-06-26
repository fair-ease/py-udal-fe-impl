# FAIR-EASE UDAL Implementation Example

See the Uniform Data Access Layer (UDAL) specification at
https://github.com/fair-ease/py-udal-interface

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
