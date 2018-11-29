''' The system related utils '''
import os

def get_metric_server_address():
    ''' Get the metric server address '''
    return os.environ.get("OPERATOR_ADDRESS", "127.0.0.1:50050")
