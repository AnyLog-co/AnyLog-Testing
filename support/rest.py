import json
import random
import sys
import support.anylog_api as anylog_api

slash_char = '/'
if sys.platform.startswith('win'):
    slash_char = '\\'


def put_data(node_config:str, file_name:str)->bool:
    """
    PUT data from file into connection
    :args:
        node_config:str - IP % Port to send data into
        file_name:str - file to upload
    :param:
        status:bool
    :return:
        status
    """
    if isinstance(node_config, list):
        conn = anylog_api.AnyLogConnect(conn=random.choice(node_config), auth=(), timeout=30)
    else:
        conn = anylog_api.AnyLogConnect(conn=node_config, auth=(), timeout=30)

    header = {
        'type': 'json',
        'dbms': 'al_smoke_test',
        'table': file_name.split('.')[1],
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }

    try:
        with open(file_name, 'r') as f:
            for line in f.read().split('\n'):
                conn.put(headers=header, payload=line)
    except:
        pass


def query_data(conn:anylog_api.AnyLogConnect, command:str, destination:str="network")->str:
    header = {
        "command": command,
        "User-Agent": "AnyLog/1.23",
        "destination": "network"
    }

    r, error = conn.get(headers=header)
    if r is not False:
        try:
            return r.json()['Query']
        except Exception as e:
            return r.text()