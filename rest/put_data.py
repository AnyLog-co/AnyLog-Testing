import os
import requests
import sys

if sys.platform.startswith('win'):
    DATA_DIR = "data\\"
else:
    DATA_DIR = "data/"

def put_data(file_info:str, conn:str, auth:tuple=(), timeout:int=30):
    """
    PUT data in AnyLog - data located in DATA_DIR
    :args: 
        file_info:str - db_name.sensor_name
        conn:str - REST connection info
        auth:tuple - REST authentication
        timeout:int - timeout 
    :params: 
        headers:dict - header for PUT requests 
        full_path:str - DATA_DIR + file_name 
        status:list - whether or not row got inserted 
    :return: 
        status 
    """
    headers = {
        'type': 'json',
        'dbms': 'anylog', 
        'table': 'ping_sensor', 
        'mode': 'file',
        'Content-Type': 'text/text'
    }

    for file_name in os.listdir(DATA_DIR):
        data = []
        if file_info in file_name:
            full_path = DATA_DIR + file_name
            try:
                with open(full_path, 'r') as f:
                    try:
                        data = str(f.readlines())
                    except Exception as e:
                        assert True == False, 'Failed to extract results (Error: %s).\n\tQuery: %s\n' % (e, query)
            except Exception as e:
                assert True == False, 'Failed to read file %s (Error: %s).\n\tQuery: %s\n' % (file_name, e, query)
            else:
                try:
                    r = requests.put('http://%s' % conn, headers=headers, auth=auth, timeout=timeout, data=data)
                except Exception as e:
                    assert True == False, 'Failed to POST data from %s on %s (Error: %s)' % (file_name, conn, e)
