import os

class Configuration():
    @staticmethod
    def get_elasticsearch_server():
        elastic_search_server='localhost'
        if os.environ.get('ELASTICSEARCH_SERVER') is not None:
            elastic_search_server=os.environ['ELASTICSEARCH_SERVER']
        return elastic_search_server


