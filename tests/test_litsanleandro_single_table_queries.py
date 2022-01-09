import datetime
import filecmp
import os
import pytest
import sys
import tests.pytest_setup_teardown as pytest_setup_teardown

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

CONFIG_FILE = os.path.join(ROOT_DIR, 'configs', 'sample_config.ini') # will be replaced by user param
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect')
ACTUAL_DIR = os.path.join(ROOT_DIR, 'actual')

sys.path.insert(0, SUPPORT_DIR)
import file_io
import send_data
import support


class TestLitSanLeandroSingleTableQueries:
    """
    Using Lit San Leandro execute tests such as:
        * count
        * distinct
        * count(distinct)
        * min
        * max
        * avg
        * sum
        * raw
        * increments
        * WHERE + period
    against data-types:
        * float
        * timestamp
        * string
        * UUID
    :tables:
        ping_sensor
    """
    def setup_class(self):
        """
        Read configs and generate list of content to store
        :data sets:
            - ping_sensor
        :args:
        :params:
            self.status - whether or not able to access AnyLog
            self.configs:dict - configurations
            payloads:list - content to save on AnyLog
        """
        self.status, self.configs, self.anylog_conn = pytest_setup_teardown.setup_code(config_file=CONFIG_FILE,
                                                                                       expected_dir=EXPECTED_DIR,
                                                                                       actual_dir=ACTUAL_DIR)
        if self.status is False:
            pytest.fail('Failed to get status against AnyLog node %s' % self.configs['conn'])

        # Insert data process
        if self.configs['insert'] == 'true':
            pytest_setup_teardown.write_data(data_dir=DATA_DIR, send_type=self.configs['send'],
                                             dbms=self.configs['dbms'], tables=['ping_sensor'],
                                             anylog_conn=self.anylog_conn, topic=self.configs['topic'],
                                             broker=self.configs['broker'], port=self.configs['port'],
                                             username=self.configs['mqtt_user'], password=self.configs['mqtt_password'])
            
    def teardown_class(self):
        # pytest_setup_teardown.teardown_code(actual_dir=ACTUAL_DIR)
        pass

    def test_row_count(self):
        """
        Check basic count(*)
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT COUNT(*) FROM ping_sensor
        :assert:
            by validating row count user validates the correct number or rows inserted
        """
        headers = {
            'command': 'sql %s format=json and stat=false "SELECT COUNT(*) FROM ping_sensor"' % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_row_count.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_row_count.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query='sql %s format=json and stat=false "SELECT COUNT(*) FROM ping_sensor"',
                                              file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct_value(self):
        """
        Execute DISTINCT against float column
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT DISTINCT(value) FROM ping_sensor
        :assert:
            Distinct vales returned
        """
        headers = {
            'command': 'sql %s format=json and stat=false "SELECT DISTINCT(value) FROM ping_sensor ORDER BY value ASC"' % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_distinct_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_distinct_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(
                        query='sql %s format=json and stat=false "SELECT DISTINCT(value) FROM ping_sensor ORDER BY value ASC"',
                        file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct_uuid(self):
        """
        Execute DISTINCT against UUID column
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT DISTINCT(parentelement) FROM ping_sensor
        :assert:
            Distinct vales returned
        """
        headers = {
            'command': 'sql %s format=json and stat=false "SELECT DISTINCT(parentelement) FROM ping_sensor ORDER BY parentelement ASC"' % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_distinct_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_distinct_uuid.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(
                        query='sql %s format=json and stat=false "SELECT DISTINCT(parentelement) FROM ping_sensor ORDER BY parentelement ASC"',
                        file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct_string(self):
        """
        Execute DISTINCT against string column
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT DISTINCT(device_name) FROM ping_sensor
        :assert:
            Distinct vales returned
        """
        headers = {
            'command': 'sql %s format=json and stat=false "SELECT DISTINCT(device_name) FROM ping_sensor ORDER BY device_name ASC"' % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_distinct_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_distinct_string.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(
                        query='sql %s format=json and stat=false "SELECT DISTINCT(device_name) FROM ping_sensor ORDER BY device_name ASC"',
                        file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

