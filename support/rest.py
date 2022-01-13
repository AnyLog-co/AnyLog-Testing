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
            pytest.fail(f'Failed to execute GET against {self.conn} (Error: {e})')
        else:
            if int(response.status_code) != 200:
                pytest.fail(f'Failed to execute GET against {self.conn} (Network Error: {response.status_code})')

        try:
            results = response.json()
        except Exception as e:
            try:
                results = response.text
            except Exception as e:
                pytest.fail(f'Failed to extract results from GET request (Error: {e})')

        return results

    def put(self, headers:dict, payload:str=None):
        """
        Send data via REST using PUT command
        :url:
            https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-put-command
        :args:
            headers:dict - REST header
            payload:str - content to put in AnyLog
        :params:
            response:requests.models.Response - results from REST PUT request
        """
        try:
            response = requests.put(url='http://%s' % self.conn, headers=headers, data=payload, auth=self.auth,
                                    timeout=self.timeout)
        except Exception as e:
            pytest.fail(f'Failed to execute PUT against {self.conn} (Error: {e})')
        else:
            if int(response.status_code) != 200:
                pytest.fail(f'Failed to execute PUT against {self.conn} (Network Error: {response.status_code})')

    def post(self, headers:dict, payload:str=None):
        """
        Send data via REST using POST command
        :url:
            https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-post-command
        :comment:
            requires MQTT client call on the accepting AnyLog side
        :args:
            headers:dict - REST header
            payload:str - content to put in AnyLog
        :params:
            headers:dict - REST header
            response:requests.models.Response - results from REST POST request
        """
        try:
            response = requests.post(url='http://%s' % self.conn, headers=headers, data=payload, auth=self.auth,
                                    timeout=self.timeout)
        except Exception as e:
            pytest.fail(f'Failed to execute POST against {self.conn} (Error: {e})')
        else:
            if int(response.status_code) != 200:
                pytest.fail(f'Failed to execute POST against {self.conn} (Network Error: {response.status_code})')


