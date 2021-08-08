import datetime 
import json 
import os 
import requests 

DATA_DIR = 'data'

def put_data(conn:str)->list: 
    """
    PUT data in AnyLog - data located in DATA_DIR
    :args: 
        conn:str - REST connection info
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
        'mode': 'streaming', 
        'Content-Type': 'text/text'
    }
    status = [] 

    for file_name in os.listdir(DATA_DIR): 
        data = []
        full_path = DATA_DIR + '/' + file_name 
        try: 
            with open(full_path, 'r') as f: 
                for row in f.readlines(): 
                    if row != '\n': 
                        try: 
                            r = requests.put('http://%s' % conn, headers=headers, data=row)
                        except Exception as e:
                            print('Failed to POST data from %s on %s (Error: %s)' % (file_name, conn, e))
                            status.append(False) 
        except Exception as e: 
            print('Failed to open file %s (Error: %s)' % (file_name, e))
            status.append(False) 
        print(len(data))

    return status 

