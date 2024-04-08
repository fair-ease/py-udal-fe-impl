import pandas as pd

from . import udal as fe


def example1():
    print('\n### Example 1 ###\n')
    queryname = 'urn:fairease.eu:udal:example:weekdays'
    print(fe.UDAL().execute(queryname).data())


def example2():
    print('\n### Example 2 ###\n')
    queryname = 'urn:fairease.eu:udal:example:weekdays'
    queryparams = { 'lang': 'en' }
    print(fe.UDAL().execute(queryname, queryparams).data(type=pd.DataFrame))


def example3():
    print('\n### Example 3 ###\n')
    queryname = 'urn:fairease.eu:udal:example:months'
    queryparams = { 'lang': ['en', 'fr'], 'format': 'long' }
    print(fe.UDAL().execute(queryname, queryparams).data())


def example4():
    print('\n### Example 4 ###\n')
    queryname = 'urn:fairease.eu:udal:example:translation'
    print(fe.UDAL().execute(queryname).data())


def example5():
    print('\n### Example 5 ###\n')
    endpoint = 'https://www.wikidata.org/'
    queryname = 'urn:fairease.eu:udal:example:weekdays'
    queryparams = { 'lang': ['en', 'fr', 'nl'] }
    print(fe.UDAL(endpoint).execute(queryname, queryparams).data())


def example6():
    print('\n### Example 6 ###\n')
    endpoint = 'https://www.wikidata.org/'
    queryname = 'urn:fairease.eu:udal:example:months'
    queryparams = { 'lang': ['en', 'fr'] }
    print(fe.UDAL(endpoint).execute(queryname, queryparams).data())


if __name__ == "__main__":

    localQueryNames = fe.UDAL().query_names
    print(f'Query names (local): {localQueryNames}')

    endpoint = 'https://www.wikidata.org/'
    endpointQueryNames = fe.UDAL(endpoint).query_names
    print(f'Query names ({endpoint}): {endpointQueryNames}')

    example1()
    example2()
    example3()
    example4()
    example5()
    example6()
