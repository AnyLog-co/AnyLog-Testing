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
        print('Failed execute query on %s (Error: %s).\n\tQuery: %s\n' % (conn, e, query))
        r = None
    else: 
        if int(r.status_code) != 200: 
            print('Failed to execute query on %s due to network error %s.\n\tQuery: %s\n' % (conn, r.status_code, query))
            r = None

    return r

def get_status(conn:str, query:str, auth:tuple=(), timeout:int=30)->bool: 
    """
    Check whether am able to get status
    :args:
        conn:str - connection
        query:str - query to execute
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
            print('Failed to extracted data from results (Error: %s).\n\tQuery: %s\n' % (e, query))
            status = False 
        else: 
            if 'running' not in output or 'not' in output:
                status = False 
    else: 
        status = False

    return status 
