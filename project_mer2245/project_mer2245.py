#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from urllib.parse import urlencode
import urllib.request, json
import requests
from bs4 import BeautifulSoup


def search_data(query):
    """
    Function that finds the title and url of datasets in the database of datasets.
    Parameters
    ---
    query: str that refers a keyword or query
    Returns
    ---
    dataframe with titles and urls of datasets
    Examples
    ---
    >>> search_data(query='311')
    title  |  url  \\
    311 data  |  https://data.cityofnewyork.us/resource/erm2-nwe9.csv?  \\
    Fire data  |  https://data.cityofnewyork.us/resource/eerw3-83uj.csv?  \\
    >>> 
    
    """
    query = str(query.replace(' ','%20'))
    response = requests.get("https://data.cityofnewyork.us/browse?limitTo=datasets&q="+str(query)+"&sortBy=relevance")
    soup = BeautifulSoup(response.content, 'html.parser')
    all_titles = [a.text for a in soup.find_all(class_="browse2-result-name-link")]
    all_url = [a.get('href') for a in soup.find_all(class_="browse2-result-name-link")]
    data = pd.DataFrame({'title':all_titles,'url':all_url})
    data['url'] = data['url'].apply(lambda x:'https://data.cityofnewyork.us/resource/'+x[-9:]+'.csv?')
    return data


def get_fields(url):
    """
    Function that gets the fields of the dataframe and one example value.
    Parameters
    ---
    url: str that refers the dataset
    Returns
    ---
    dataframe with fields and examples
    Examples
    ---
    >>> get_fields(url='https://data.cityofnewyork.us/resource/erm2-nwe9.csv?')
    0 |  1  \\
    agency  |  DOT  \\
    complaint  |  noise  \\
    borough  |  BRONX  \\
    created_date  |  2020-12-12  \\
    >>> 
    
    """
    query = {'$select': '*',
             '$limit': 1}

    data = pd.read_csv(url + urlencode(query))
    data = data.transpose()
    return data


def get_values(url,field):
    """
    Function that gets unique values from a specific dataset with a specific field.
    Parameters
    ---
    url: str that refers the dataset
    field: str that refers the field to obtain unique values
    Returns
    ---
    str list with all unique values in that field
    Examples
    ---
    >>> get_values(url='https://data.cityofnewyork.us/resource/erm2-nwe9.csv?',field='agency')
    ['DOT','NYPD','HPD',...]
    >>> 
    
    """

    query = {'$select': 'DISTINCT('+str(field)+')',
             '$limit': 10}

    data = pd.read_csv(url + urlencode(query))
    columns = list(data.values)
    return columns


def filters_cleaner(filters):
    """
    Function that clean the filters for easy_load.
    Parameters
    ---
    filters: dict with the filters
    Returns
    ---
    str with the exact query for SODA API
    Examples
    ---
    >>> filters_clean(filters={'agency':['DOT'],'borough':['BRONX']})
    'agency = "DOT" and boro =  "BRONX"'
    >>> 
    
    """
    filters_ls = []
    for key,value in filters.items():
        filters_ls_temp = []
        for i in value:
            filters_ls_temp.append(str(key + '=' +'"' +i+'"'))
        filters_str_temp = " or ".join(filters_ls_temp)
        filters_ls.append(filters_str_temp)
    filters_str = " and ".join(filters_ls)
    return filters_str


def easy_load(url,fields='*',filters=None,limit=10):
    """
    Function that downloads data with especific requirements.
    Parameters
    ---
    url: string that refers to the dataset
    fields: list that are of interest
    filters: dict that filter the results
    limit: int that limits the number of results
    Returns
    ---
    dataframe with data from the variables
    Examples
    ---
    >>> easy_load(url='https://data.cityofnewyork.us/resource/erm2-nwe9.csv?',fields=['unique_key','agency','borough'],
                    filters={'agency':['DOT'],'borough':['BRONX']})
    unique_key |  agency  |  borough  \\
    6789788  |  DOT  | BRONX  \\
    >>> 
    
    """
    fields = ','.join(fields)
    if filters == None:
        query = {'$select': fields,
             '$limit': limit}
    else:
        filters = filters_cleaner(filters)
        query = {'$select': fields,
                 '$where': filters,
                 '$limit': limit}

    data = pd.read_csv(url + urlencode(query))
    return data


def quick_load(query):
    """
    Function that downloads data with the first result that appears.
    Parameters
    ---
    query: string keyword to search
    Returns
    ---
    dataframe with data from the keyword
    Examples
    ---
    >>> quick_load(query='311')
    unique_key |  created_date  |  closed_date  |  agency  \\
    6789788  |  2020-12-12  | NaN  |  NYPD \\
    >>> 
    
    """
    dataframe = search_data(query)
    url = dataframe.iloc[0,1]
    result = easy_load(url)
    return result
