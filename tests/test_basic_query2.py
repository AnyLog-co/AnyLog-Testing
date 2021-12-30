import datetime
import os
import pytest
import sys

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
print(ROOT_DIR)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

CONFIG_FILE = os.path.join(ROOT_DIR, 'configs', 'sample_config.ini') # will be replaced by user param

sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest_get
import send_data
import support

class TestBasicQueries2:
    """
    The following tests basic functions using WHERE / GROUP BY / ORDER BYagainst the following data types:
        * float
        * timestamp
        * string
        * UUID
    """
    def setup(self):
        """
        Read configs and generate list of content to store
        :data sets:
            - ping_sensor
        :args:
        :params:
            self.status - whether or not able to access AnyLog
            self.configs:dict - configurations
            self.payloads:list - content to save on AnyLog
        """
        self.payloads = []
        self.configs = file_io.read_configs(config_file=CONFIG_FILE)
        self.configs['table'] = 'ping_sensor'
        for fn in os.listdir(DATA_DIR):
            if 'ping_sensor' in fn:
                file_name = os.path.join(DATA_DIR, fn)
                self.payloads += file_io.read_file(file_name=file_name)

        if self.configs['insert'] == 'true':
            send_data.store_payloads(payloads=self.payloads, configs=self.configs)

        self.status = rest_get.get_status(conn=self.configs['conn'], username=self.configs['rest_user'],
                                          password=self.configs['rest_password'])

    def test_data_summary(self):
        """
        Get summary of the data
        :query:
            SELECT min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor;
        :assert:
            correct results
        """
        query = "SELECT min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor;"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])

            if isinstance(output, dict):
                try:
                    result = output['Query'][0]
                except Exception as e:
                    pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert result == {'min(timestamp)': '2021-12-30 20:44:18.878658',
                                      'max(timestamp)': '2021-12-30 20:45:58.142131',
                                      'min(value)': '0.22',
                                      'avg(value)': '16.5806',
                                      'max(value)': '48.14'}
            else:
                pytest.fail('Failed to validate connection to AnyLog')

    def test_data_summary_by_uuid_desc(self):
        """
        Get summary of the data based on UUID column
        :query:
            SELECT parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement DESC;
        :assert:
            correct results
        """
        query = "SELECT parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement DESC;"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])

            if isinstance(output, dict):
                try:
                    result = output['Query']
                except Exception as e:
                    pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert result == [{'parentelement': 'f0bd0832-a81e-11ea-b46d-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:23.894585', 'max(timestamp)': '2021-12-30 20:45:55.130598', 'min(value)': '2.09', 'avg(value)': '17.849545454545453', 'max(value)': '36.46'},
                                      {'parentelement': 'd515dccb-58be-11ea-b46d-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:18.878658', 'max(timestamp)': '2021-12-30 20:45:57.138328', 'min(value)': '0.78', 'avg(value)': '25.590833333333332', 'max(value)': '48.14'},
                                      {'parentelement': '68ae8bef-92e1-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:20.885200', 'max(timestamp)': '2021-12-30 20:45:44.101509', 'min(value)': '15.13', 'avg(value)': '34.972142857142856', 'max(value)': '47.72'},
                                      {'parentelement': '62e71893-92e0-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:24.897399', 'max(timestamp)': '2021-12-30 20:45:56.133286', 'min(value)': '0.22', 'avg(value)': '2.1544444444444446', 'max(value)': '3.95'},
                                      {'parentelement': '1ab3b14e-93b1-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:19.882427', 'max(timestamp)': '2021-12-30 20:45:58.142131', 'min(value)': '0.32', 'avg(value)': '5.581818181818182', 'max(value)': '10.31'}]
            else:
                pytest.fail('Failed to validate connection to AnyLog')

    def test_data_summary_by_uuid_asc(self):
        """
        Get summary of the data based on UUID column
        :query:
            SELECT parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement ASC;
        :assert:
            correct results
        """
        query = "SELECT parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement ASC;"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])

            if isinstance(output, dict):
                try:
                    result = output['Query']
                except Exception as e:
                    pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert result == [{'parentelement': '1ab3b14e-93b1-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:19.882427', 'max(timestamp)': '2021-12-30 20:45:58.142131', 'min(value)': '0.32', 'avg(value)': '5.581818181818182', 'max(value)': '10.31'},
                                      {'parentelement': '62e71893-92e0-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:24.897399', 'max(timestamp)': '2021-12-30 20:45:56.133286', 'min(value)': '0.22', 'avg(value)': '2.1544444444444446', 'max(value)': '3.95'},
                                      {'parentelement': '68ae8bef-92e1-11e9-b465-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:20.885200', 'max(timestamp)': '2021-12-30 20:45:44.101509', 'min(value)': '15.13', 'avg(value)': '34.972142857142856', 'max(value)': '47.72'},
                                      {'parentelement': 'd515dccb-58be-11ea-b46d-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:18.878658', 'max(timestamp)': '2021-12-30 20:45:57.138328', 'min(value)': '0.78', 'avg(value)': '25.590833333333332', 'max(value)': '48.14'},
                                      {'parentelement': 'f0bd0832-a81e-11ea-b46d-d4856454f4ba', 'min(timestamp)': '2021-12-30 20:44:23.894585', 'max(timestamp)': '2021-12-30 20:45:55.130598', 'min(value)': '2.09', 'avg(value)': '17.849545454545453', 'max(value)': '36.46'}]
            else:
                pytest.fail('Failed to validate connection to AnyLog')
