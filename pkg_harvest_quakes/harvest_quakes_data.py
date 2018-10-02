import requests
from elasticsearch import Elasticsearch

import json
from math import ceil
import sys
from configuration import Configuration
import datetime

es = Elasticsearch([{'host': Configuration.get_elasticsearch_server(), 'port': 9200}])


def harvest_earthquakes(period):
    """ method to get the earthquakes from usgs """
    url = None
    dict_response = {}
    st_datetime_now = str(datetime.datetime.utcnow().isoformat())
    line = st_datetime_now + ' : request => ' + period + '\n'
    Configuration.write_to_log('-----------------------------------' + '\n')
    Configuration.write_to_log(line)

    if period in ['last_hour', 'last_day','last_week', 'last_month']:

        if period == 'last_hour':
            url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
        elif period == 'last_day':
            url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
        elif period == 'last_week':
            url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson'
        elif period == 'last_month':
            url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
        if url is not None:
            r = requests.get(url)
            response_as_dict = json.loads(r.content)
            features = response_as_dict['features']
            dict_response['features'] = features
            dict_response['result'] = 'success'
            print('successfully retrieved ' + str(len(dict_response['features'])))
            line='successfully retrieved ' + str(len(dict_response['features'])) + '\n'
            Configuration.write_to_log(line)

            load_data_in_es(dict_response['features'], 'usgs')

    elif period in ['2018']:
        harvest_quakes_by_time_period()
    elif period in ['delete_index']:
        """ special operation to delete an index """
        delete_by_index('usgs')

    else:
        dict_response['response'] = 'fail'
        dict_response['error'] = 'The keyword provided does not correspond to any valid period.'
        return dict_response


def load_data_in_es(ls_docs, index):
    stResponse=''
    ls_indexes = list_indexes()
    if index in ls_indexes:
        print('the index already exist')

    else:
        print('the index does not exist, i will create one')
        es.indices.create(index=index, ignore=400)

    query_count = {"query": {"match_all": {}}}
    count = es.count(index=index, body=query_count)
    stResponse=stResponse+ 'number of features in ' + index + ' :' + str(count['count']) + '\n'

    i = 1
    for d in ls_docs:
        d=add_days_to_data(d)

        geometry=d['geometry']['coordinates']
        lat=geometry[1]
        lng=geometry[0]

        d['properties']['lat']=lat
        d['properties']['lng'] = lng

        es.index(index=index, doc_type='quake_summary', id=d['id'], body=d)

    count = es.count(index=index, body=query_count)

    stResponse=stResponse+'after loading data, number of features in ' + index + ' :' + str(count['count']) + '\n'
    Configuration.write_to_log(stResponse)


def search_quakes(keyword):
    """ search one earthquake by id """
    dict_query = {"from":0, "size":1000 ,"query": {"match": {'properties.place': keyword}}}
    y = es.search(index='usgs', body=dict_query)
    print(json.dumps(y, indent=4, sort_keys=True))

def list_indexes():
    """
        An index is a collection of documents that have somewhat similar characteristics.
    For example, you can have an index for customer data, another index for a product catalog,
    and yet another index for order data. An index is identified by a name (that must be all lowercase)
    and this name is used to refer to the index when performing indexing, search, update, and delete
    operations against the documents in it.
    """
    ls_indices = es.indices.get_alias("*")
    return ls_indices

def delete_by_index(index):
    """
    to delete an index,
    """
    try:
        es.indices.delete(index=index, ignore=[400, 4004])
        print('the index ' + index + ' has been eliminated.')
    except Exception as err:
        print(err)

def add_days_to_data(dict_quake):
    number_miliseconds_per_day=86400000
    days= ceil(dict_quake['properties']['time']/number_miliseconds_per_day)
    dict_quake['properties']['days']=days
    return dict_quake

def update_values_quakes(ls_quakes, index):
    for q in ls_quakes:
        updated_q=add_days_to_data(q['_source'])
        es.update(index=index, doc_type='quake_summary', id=updated_q['id'], body={"doc":updated_q})

def search_quakes_by_keyword_place(index, keyword):
    print ('testing query by keyword')
    dict_query={"from":0, "size":1000,"query":{"match":{'properties.place':keyword}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def get_all_docs(index,number_of_docs):
    dict_query={"from":0, "size":number_of_docs ,"query":{"match_all":{}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def get_docs_with_no_days(index, number_of_docs):
    dict_query = {"from": 0, "size": number_of_docs, "query": {"bool":{"must_not":{"exists":{"field":"properties.days"}}}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query


def get_docs_with_no_position_in_properties(index, number_of_docs):
    dict_query = {"from": 0, "size": number_of_docs, "query": {"bool":{"must_not":{"exists":{"field":"properties.lat"}}}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def update_value_position_quakes(ls_quakes, index):
    print('elements to update', len(ls_quakes))
    for q in ls_quakes:
        geometry=q['_source']['geometry']['coordinates']
        lat=geometry[1]
        lng=geometry[0]
        updated_q=q['_source']
        updated_q['properties']['lat']=lat
        updated_q['properties']['lng'] = lng
        es.update(index=index, doc_type='quake_summary', id=updated_q['id'], body={"doc":updated_q})



def get_counts_per_day(index, keyword):
    dict_query={"size":0,"aggs":{"group_by_day":{"terms":{"field":"properties.days"}}}, "query":{"match":{'properties.place':keyword}}}
    result_query = es.search(index=index,body=dict_query)
    return result_query

def harvest_quakes_by_time_period(year):
    """
        special operation harvest quakes from specific time periods
        the function harvest_quakes_by_time_period() contains a list of urls for the months from january to november 2018
        It will request and upload the data into elasticsearch for those periods.
    """

    ls_requests=[]
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-01-01 00:00:00&endtime='+str(year)+'-01-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-01-15 00:00:00&endtime='+str(year)+'-02-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-02-01 00:00:00&endtime='+str(year)+'-02-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-02-15 00:00:00&endtime='+str(year)+'-03-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-03-01 00:00:00&endtime='+str(year)+'-03-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-03-15 00:00:00&endtime='+str(year)+'-04-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-04-01 00:00:00&endtime='+str(year)+'-04-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-04-15 00:00:00&endtime='+str(year)+'-05-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-05-01 00:00:00&endtime='+str(year)+'-05-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-05-15 00:00:00&endtime='+str(year)+'-06-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-06-01 00:00:00&endtime='+str(year)+'-06-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-06-15 00:00:00&endtime='+str(year)+'-07-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-07-01 00:00:00&endtime='+str(year)+'-07-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-07-15 00:00:00&endtime='+str(year)+'-08-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-08-01 00:00:00&endtime='+str(year)+'-08-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-08-15 00:00:00&endtime='+str(year)+'-09-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-09-01 00:00:00&endtime='+str(year)+'-09-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-09-15 00:00:00&endtime='+str(year)+'-10-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-10-01 00:00:00&endtime='+str(year)+'-10-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-10-15 00:00:00&endtime='+str(year)+'-11-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-11-01 00:00:00&endtime='+str(year)+'-11-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-11-15 00:00:00&endtime='+str(year)+'-12-01 00:00:00&minmagnitude=0&orderby=time')

    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-12-01 00:00:00&endtime='+str(year)+'-12-15 00:00:00&minmagnitude=0&orderby=time')
    ls_requests.append('https://earthquake.usgs.gov/fdsnws/event/1/query.geojson?starttime='+str(year)+'-12-15 00:00:00&endtime='+str(year)+'-12-31 24:00:00&minmagnitude=0&orderby=time')

    dict_response={}
    for url in ls_requests:
        Configuration.write_to_log(url + '\n')
        r = requests.get(url)
        response_as_dict = json.loads(r.content)
        load_data_in_es(response_as_dict['features'], 'usgs')

if __name__ == '__main__':
    print (sys.argv)
    period=sys.argv[1]
    print('selected period: ',period)

    #--------------------------------------
    """ normal operation """
    harvested_quakes = harvest_earthquakes(period)
    #--------------------------------------

    #ls_quakes=get_docs_with_no_position_in_properties('usgs',2000)
    #update_value_position_quakes(ls_quakes['hits']['hits'], 'usgs')


    #---------------------------------------------------
    """special operation, to update the key days to quakes"""
    # ls_quakes =get_docs_with_no_days('usgs',2000)
    # print (len(ls_quakes['hits']['hits']))
    # print(ls_quakes['hits']['hits'])
    # update_values_quakes(ls_quakes['hits']['hits'], 'usgs')
    #---------------------------------------------------


