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
    else:
        if int(response.status_code) != 200:
            pytest.fail('Failed to execute GET against %s (Network Error: %s)' % (conn, result.status_code))
            response = None

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
            if 'running' in result and 'not' not in result:
                status = True

    return status
