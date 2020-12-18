from project_mer2245 import __version__
from project_mer2245 import project_mer2245
import pytest

def test_version():
    assert __version__ == '0.1.0'

def test_search_data():
    example = '311'
    expected = 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv?'
    data = project_mer2245.search_data(example)
    actual = data.iloc[0,1]
    assert actual == expected, 'Test not passed'

def test_filters_cleaner():
    example = {'agency':['DOT'],'borough':['BRONX']}
    expected = 'agency="DOT" and borough="BRONX"'
    actual = project_mer2245.filters_cleaner(example)
    assert actual == expected, 'Test not passed'