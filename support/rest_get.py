import textwrap

import pytest
import requests


def __get_command(conn:str, headers:dict, auth:tuple=None, timeout:int=30)->requests.models.Response:
    """
    Generic GET request
    :args:
        conn:str - REST IP & Port
        headers:dict - REST headers
        auth:tuple - authentication information
        timeout:int - REST request timeout
    :params:
        response:requests.models.Response' - results from REST GET request
    :return:
        response
    """
    try:
        response = requests.get(url='http://%s' % conn, headers=headers, auth=auth, timeout=timeout)
    except Exception as e:
        pytest.fail('Failed to execute GET against %s (Error: %s)' % (conn ,e))
        response = None

    assert response is not None
    if response is not None:
        assert int(response.status_code) == 200

    return response


def get_status(conn:str, username:str='', password='', timeout:int=30)->bool:
    """
    Execute `get status` command against AnyLog
    :args:
        conn:str - REST IP + Port
        username:str - REST authentication user
        password:str - REST authentication password
        timeout:int - REST timeout
    :params:
        status:bool
        headers:dict - REST header information
        auth:tuple - username + password authentication
        response:requests.models.Response - results form GET command
        result:str - raw content
    :return:
        status
    """
    status = False
    headers = {
        'command': 'get status',
        'User-Agent': 'AnyLog/1.23'
    }

    auth = None
    if username != '' and password != '':
        auth = (username, password)

    response = __get_command(conn=conn, headers=headers, auth=auth, timeout=timeout)
    if response is not None:
        try:
            result = response.text
        except Exception as e:
            pytest.fail('Failed to extract results from response (Error: %s)' % e)
        else:
            assert 'running' in result and 'not' not in result
            if 'running' in result and 'not' not in result:
                status = True
    return status


def get_basic(conn:str, dbms:str, query:str, username:str='', password='', timeout:int=30)->list:
    """
    Execute GET based on query
    :args:
        conn:str - REST IP + Port
        dbms:str - logical database to query
        query:str - query to execute
        username:str - REST authentication user
        password:str - REST authentication password
        timeout:int - REST timeout
    :params:
        headers:dict - REST header information
        auth:tuple - username + password authentication
        response:requests.models.Response - results form GET command
        result:str - raw content
    :return:

    """
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (dbms, query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    auth = None
    if username != '' and password != '':
        auth = (username, password)

    response = __get_command(conn=conn, headers=headers, auth=auth, timeout=timeout)
    try:
        result = response.json()
    except Exception as e:
        try:
            result = response.text
        except Exception as e:
            pytest.fail('Failed to extract results (Error: %s)' % e)
            result = None
    return result


def get_complex(conn:str, dbms:str, query:str, username:str='', password='', timeout:int=30)->list:
    """
    Execute GET based on query
    :args:
        conn:str - REST IP + Port
        dbms:str - logical database to query
        query:str - full query (including sql params) 
        username:str - REST authentication user
        password:str - REST authentication password
        timeout:int - REST timeout
    :params:
        headers:dict - REST header information
        auth:tuple - username + password authentication
        response:requests.models.Response - results form GET command
        result:str - raw content
    :return:

    """
    headers = {
        'command': query,
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    auth = None
    if username != '' and password != '':
        auth = (username, password)

    response = __get_command(conn=conn, headers=headers, auth=auth, timeout=timeout)
    try:
        result = response.json()
    except Exception as e:
        try:
            result = response.text
        except Exception as e:
            pytest.fail('Failed to extract results (Error: %s)' % e)
            result = None
    return result