import requests

def get(conn:str, query:str, remote:str=False, auth:tuple=(), timeout:int=30)->requests.models.Response: 
    """
    Execute GET requests
    :args:
        conn:str - connection
        query:str - query to execute
        remote:str - whether query is remote or note
        auth:tuple - REST authentication
        timeout:int - timeout 
    :params: 
        headers:dict - headers to execute 
        r:requests.models.Response - results from request
    :return: 
        r
    """
    headers = {
        'command': query,
        'User-Agent': 'AnyLog/1.23'
    }
    if remote == True: 
        headers['destination'] = 'network'

    try: 
        r = requests.get('http://%s' % conn, headers=headers, auth=auth, timeout=timeout)
    except Exception as e: 
        assert True == False, 'Failed execute query on %s (Error: %s).\n\tQuery: %s\n' % (conn, e, query)
    else: 
        if int(r.status_code) != 200: 
            assert True == False, 'Failed to execute query on %s due to network error %s.\n\tQuery: %s\n' % (conn, r.status_code, query)

    return r

def get_status(conn:str, auth:tuple=(), timeout:int=30)->bool: 
    """
    Check whether am able to get status
    :args:
        conn:str - connection
        auth:tuple - REST authentication
        timeout:int - timeout 
    :params: 
        status:bool 
        r:requests.models.Response - results from request
        output:str - extracted data from requst 
        cmd:str - command to execute
    :return: 
        status
    """
    status = True
    cmd = 'get status'

    r = get(conn=conn, query=cmd, auth=auth, timeout=timeout)
    if r != None: 
        try: 
            output = r.text
        except Exception as e: 
            assert True == False, 'Failed to extracted data from results (Error: %s).\n\tQuery: %s\n' % (e, query)
        else: 
            if 'running' not in output or 'not' in output:
                assert True == False, 'Unable to connect to AnyLog. Recieved message: %s.\n\tQuery: %s\n' % (r.text, query) 

    return True

def get_json(conn:str, query:str, remote:str=True, auth:tuple=(), timeout:int=30)->list: 
    """
    Execute GET query & extract results
    :args:
        conn:str - connection
        query:str - query to execute
        remote:str - whether query is remote or note
        auth:tuple - REST authentication
        timeout:int - timeout 
    :params: 
        r:requests.models.Response - results from request
        raw_data:dict - raw data 
        output:list - data extracted
    :return: 
        output
    """
    output = [] 
    r = get(conn=conn, query=query, remote=remote, auth=auth, timeout=timeout)
    if r != None: 
        try: 
            raw_data= r.json()
        except Exception as e: 
            assert True == False, 'Failed to extracted JSON data (Error: %s).\n\tQuery: %s\n' % (e, query)
        else: 
            if 'Query' in raw_data: 
                output = raw_data['Query'] 
            else: 
                assert True == False, 'Failed extract data (Error: %s).\n\tQuery; %s\n' % (raw_data, query) 

    return output 

