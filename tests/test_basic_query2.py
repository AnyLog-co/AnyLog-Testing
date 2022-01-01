import datetime
import filecmp
import os
import pytest
import sys

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

EXPECT_RESULTS = os.path.join(ROOT_DIR, 'expect')
if not os.path.isdir(EXPECT_RESULTS):
    os.makedirs(EXPECT_RESULTS)
ACTUAL_RESULTS = os.path.join(ROOT_DIR, 'actual')
if not os.path.isdir(ACTUAL_RESULTS):
    os.makedirs(ACTUAL_RESULTS)

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
                self.payloads += file_io.read_file(file_name=file_name, dbms=self.configs['dbms'])

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

    def test_data_summary_by_string_desc(self):
        """
        Get summary of the data based on string column
        :query:
            SELECT device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY device_name ORDER BY device_name DESC;
        :assert:
            correct results
        """
        query = "SELECT device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value) FROM ping_sensor GROUP BY device_name ORDER BY device_name DESC;"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])

            if isinstance(output, dict):
                try:
                    result = output['Query']
                except Exception as e:
                    pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert result == [{'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-12-30 20:44:19.882427', 'max(timestamp)': '2021-12-30 20:45:58.142131', 'min(value)': '0.32', 'avg(value)': '5.581818181818182', 'max(value)': '10.31'},
                                      {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-12-30 20:44:18.878658', 'max(timestamp)': '2021-12-30 20:45:57.138328', 'min(value)': '0.78', 'avg(value)': '25.590833333333332', 'max(value)': '48.14'},
                                      {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-12-30 20:44:23.894585', 'max(timestamp)': '2021-12-30 20:45:55.130598', 'min(value)': '2.09', 'avg(value)': '17.849545454545453', 'max(value)': '36.46'},
                                      {'device_name': 'Catalyst 3500XL', 'min(timestamp)': '2021-12-30 20:44:20.885200', 'max(timestamp)': '2021-12-30 20:45:44.101509', 'min(value)': '15.13', 'avg(value)': '34.972142857142856', 'max(value)': '47.72'},
                                      {'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-12-30 20:44:24.897399', 'max(timestamp)': '2021-12-30 20:45:56.133286', 'min(value)': '0.22', 'avg(value)': '2.1544444444444446', 'max(value)': '3.95'}]
            else:
                pytest.fail('Failed to validate connection to AnyLog')

    def test_timestamp_less_then(self):
        """
        Test less than an actual timestamp
        :sql:
            SELECT timestamp, value from ping_sensor WHERE timestamp < '2021-12-30 20:45:26.053476' ORDER BY TIMESTAMP;
        :assert:
            results in expected file to actual_file
        """
        query = "SELECT timestamp, value from ping_sensor WHERE timestamp < '2021-12-30 20:45:26.053476' ORDER BY TIMESTAMP;"
        expect_file_name = os.path.join(EXPECT_RESULTS, 'basic_query2_test_timestamp_less_then.json')
        actual_file_name = os.path.join(ACTUAL_RESULTS, 'basic_query2_test_timestamp_less_then.json')
        try:
            open(actual_file_name, 'w')
        except Exception as e:
            pytest.fail('Failed to create results file: %s (Error: %s)' % (file_name, e))
        else:
            if self.status is True:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                            username=self.configs['rest_user'], password=self.configs['rest_password'])
                try:
                    with open(actual_file_name, 'w') as f:
                        for row in output['Query']:
                            try:
                                if row == output['Query'][-1]:
                                    f.write(support.json_dumps(row))
                                else:
                                    f.write(support.json_dumps(row) + '\n')
                            except Exception as e:
                                pytest.fail('Failed to write row to file %s (Error: %s)' % (actual_file_name, e))
                except Exception as e:
                    pytest.fail('Failed to open file %s (Error: %s)' % (actual_file_name, e))
                else:
                    assert filecmp.cmp(actual_file_name, expect_file_name)
            else:
                pytest.fail('Failed to validate connection to AnyLog')

    def test_timestamp_equal_or_less_then(self):
        """
        Test less than an actual timestamp
        :sql:
            SELECT timestamp, value from ping_sensor WHERE timestamp < '2021-12-30 20:45:26.053476' ORDER BY TIMESTAMP;
        :assert:
            results in expceted file to actual_file
        """
        query = "SELECT timestamp, value from ping_sensor WHERE timestamp <= '2021-12-30 20:45:26.053476' ORDER BY TIMESTAMP;"
        expect_file_name = os.path.join(EXPECT_RESULTS, 'basic_query2_test_timestamp_equal_or_less_then.json')
        actual_file_name = os.path.join(ACTUAL_RESULTS, 'basic_query2_test_timestamp_equal_or_less_then.json')
        try:
            open(actual_file_name, 'w')
        except Exception as e:
            pytest.fail('Failed to create results file: %s (Error: %s)' % (file_name, e))
        else:
            if self.status is True:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                            username=self.configs['rest_user'], password=self.configs['rest_password'])
                try:
                    with open(actual_file_name, 'w') as f:
                        for row in output['Query']:
                            try:
                                if row == output['Query'][-1]:
                                    f.write(support.json_dumps(row))
                                else:
                                    f.write(support.json_dumps(row) + '\n')
                            except Exception as e:
                                pytest.fail('Failed to write row to file %s (Error: %s)' % (actual_file_name, e))
                except Exception as e:
                    pytest.fail('Failed to open file %s (Error: %s)' % (actual_file_name, e))
                else:
                    assert filecmp.cmp(actual_file_name, expect_file_name)
            else:
                pytest.fail('Failed to validate connection to AnyLog')
