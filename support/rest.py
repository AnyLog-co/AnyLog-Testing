import pytest
import requests
import support

class RestCode:
    def __init__(self, conn:str, user:str='', password:str='', timeout:int=30):
        """
        Code to communicate with AnyLog via REST
        :args:
            conn:str - REST IP & Port
            user:str - REST authentication user
            password:str - REST authentication password
            timeout:int - REST timeout
        :params:
            self.conn:str - REST IP & Port
            self.auth:tuple - REST authentication
            self.timeout:int - REST timeout
        """
        self.conn = conn
        self.auth = (user, password)
        self.timeout = timeout

    def get(self, headers:dict)->str:
        """
        Execute GET request
        :args:
            headers:dict - REST headers
        :params:
            response:requests.models.Response - results from REST GET request
            results:str - results from query either as JSON or string
        :return:
            results 
        """
        try:
            response = requests.get(url='http://%s' % self.conn, headers=headers, auth=self.auth, timeout=self.timeout)
        except Exception as e:
            pytest.fail('Failed to execute GET against %s (Error: %s)' % (self.conn, e))
        else:
            if int(response.status_code) != 200:
                pytest.fail('Failed to execute GET against %s (Network Error: %s)' % (self.conn, response.status_code))

        try:
            results = response.json()
        except Exception as e:
            try:
                results = response.text
            except Exception as e:
                pytest.fail('Failed to extract results from GET request (Error: %s)' % e)

        return results

    def put(self, payloads:list, dbms:str, table:str):
        """
        Send data via REST using PUT command
        :url:
            https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-put-command
        :args:
            payloads:list - list of dicts to store in AnyLog
            dbms:str - logical database to store data in
            table:str - table to store data in
        :params:
            headers:dict - REST header
            response:requests.models.Response - results from REST GET request
        """
        headers = {
            'type': 'json',
            'dbms': dbms,
            'table': table,
            'mode': 'streaming',
            'Content-Type': 'text/plain'
        }
        for payload in payloads:
            try:
                response = requests.put(url='http://%s' % self.conn, headers=headers, data=support.json_dumps(payload),
                                        auth=self.auth, timeout=self.timeout)
            except Exception as e:
                pytest.fail('Failed to execute PUT against %s (Error: %s)' % (self.conn, e))
            else:
                if int(response.status_code) != 200:
                    pytest.fail(
                        'Failed to execute PUT against %s (Network Error: %s)' % (self.conn, response.status_code))
    
    def post(self, payloads:list, dbms:str, table:str, topic:str):
        """
        Send data via REST using POST command
        :url:
            https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-post-command
        :comment:
            requires MQTT client call on the accepting AnyLog side
        :args:
            payloads:list - list of dicts to store in AnyLog
            dbms:str - logical database to store data in
            table:str - table to store data in
            topic:str - MQTT topic
        :params:
            headers:dict - REST header
            response:requests.models.Response - results from REST GET request
        """
        headers = {
            'command': 'data',
            'topic': topic,
            'User-Agent': 'AnyLog/1.23',
            'Content-Type': 'text/plain'
        }
        for payload in payloads:
            payload['dbms'] = dbms
            payload['table'] = table
            try:
                response = requests.post(url='http://%s' % self.conn, headers=headers, data=support.json_dumps(payload),
                                         auth=self.auth, timeout=self.timeout)
            except Exception as e:
                pytest.fail('Failed to execute POST against %s (Error: %s)' % (self.conn, e))
            else:
                if int(response.status_code) != 200:
                    pytest.fail(
                        'Failed to execute POST against %s (Network Error: %s)' % (self.conn, response.status_code))

