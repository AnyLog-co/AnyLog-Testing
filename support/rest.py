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
    status = True
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
                r = conn.put(headers=header, payload=line)
    except Exception as e:
        print(e)
    else:
        if int(r.status_code) != 200:
            status = False
    return status

def post_data(node_config:str, cmd:str=None, file_name:str=None):
    """
    Post data into AnyLog
    :args:
        node_config:str - node IP & Port
        cmd:str - (mqtt) command correlated to the data set
        file_name:str - file containing data
    :params:

    """
    if isinstance(node_config, list):
        conn = anylog_api.AnyLogConnect(conn=random.choice(node_config), auth=(), timeout=30)
    else:
        conn = anylog_api.AnyLogConnect(conn=node_config, auth=(), timeout=30)

    header = {
        "command": cmd,
        "User-Agent": "AnyLog/1.23"
    }
    conn.post(headers=header)

    headers = {
        'command': 'data',
        'User-Agent': 'AnyLog/1.23',
        'Content-Type': 'text/plain'
    }

    with open(file_name, 'r') as f:
        for line in f.read().split('\n'):
            print(type(line))
            conn.post(headers=header, payload=line)



def get_status(conn:anylog_api.AnyLogConnect)->bool:
    """
    Validate connection
    :args:
        conn:anylog_api.AnyLogConnect - connection to AnyLog
    :params:
        status:bool - status
        header:dict - REST header info
    :return:
        if able to get connection and running return True else False
    """
    status = False
    header = {
        "command": "get status",
        "User-Agent": "AnyLog/1.23"
    }
    r, error = conn.get(headers=header)
    if r is not False:
        try:
            if 'running' in r.text:
                status = True
        except:
            pass
    return status


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

