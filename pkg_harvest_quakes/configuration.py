import os

class Configuration():
    @staticmethod
    def get_elasticsearch_server():
        elastic_search_server='localhost'
        if os.environ.get('ELASTICSEARCH_SERVER') is not None:
            elastic_search_server=os.environ['ELASTICSEARCH_SERVER']
        return elastic_search_server

    @staticmethod
    def write_to_log(text_to_logs):
        current_dir=os.path.dirname(os.path.realpath(__file__))
        log_file=os.path.join(current_dir, 'harvest.logs')
        if os.path.isfile(log_file):
            with open(log_file, 'a') as logs:
                logs.write(text_to_logs)
        else:
            logs = open(log_file, "w+")
            logs.write(text_to_logs)
            logs.close()



