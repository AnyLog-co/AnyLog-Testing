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
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect', 'lit_san_leandro_single_table_queries')
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
            query:str - query to execute
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT COUNT(*) FROM ping_sensor
        :assert:
            by validating row count user validates the correct number or rows inserted
        """
        query = 'sql %s format=json and stat=false "SELECT COUNT(*) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
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
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_min_value(self):
        """
        Test MIN aggregate against value
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT MIN(value) FROM ping_sensor
        :assert:
            min value returned
        """
        query = 'sql %s format=json and stat=false "SELECT MIN(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_min_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_min_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'MIN(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'MIN(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_avg_value(self):
        """
        Test AVG aggregate against value
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT AVG(value) FROM ping_sensor
        :assert:
            avg value returned
        """
        query = 'sql %s format=json and stat=false "SELECT AVG(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_avg_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_avg_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'AVG(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'AVG(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_max_value(self):
        """
        Test MAX aggregate against value
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT MAV(value) FROM ping_sensor
        :assert:
            max value returned
        """
        query = 'sql %s format=json and stat=false "SELECT MAX(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_max_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_max_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'MAX(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'MAX(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_sum_value(self):
        """
        Test SUM aggregate against value
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT SUM(value) FROM ping_sensor
        :assert:
            avg value returned
        """
        query = 'sql %s format=json and stat=false "SELECT SUM(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_sum_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_sum_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'SUM(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'SUM(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_distinct_value(self):
        """
        Execute DISTINCT against float column
        :params:
            query:str - query to execute
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT DISTINCT(value) FROM ping_sensor
        :assert:
            Distinct vales returned
        """
        query = 'sql %s format=json and stat=false "SELECT DISTINCT(value) FROM ping_sensor ORDER BY value ASC"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_distinct_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_count_distinct_value(self):
        """
        Execute COUNT-DISTINCT against float column
        :params:
            query:str - query to execute
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT COUNT(DISTINCT(value)) FROM ping_sensor
        :assert:
            count distinct vales returned
        """
        query = 'sql %s format=json and stat=false "SELECT COUNT(DISTINCT(value)) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_count_distinct_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_count_distinct_value.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(value))' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(value))' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_distinct_uuid(self):
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
        query = ('sql %s format=json and stat=false "SELECT DISTINCT(parentelement) FROM ping_sensor '
                 +'ORDER BY parentelement ASC"')
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_distinct_uuid.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_distinct_string(self):
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
        query = 'sql %s format=json and stat=false "SELECT DISTINCT(device_name) FROM ping_sensor ORDER BY device_name ASC"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_distinct_string.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_min_timestamp(self):
        """
        Test MIN aggregate against timestamp
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT MIN(value) FROM ping_sensor
        :assert:
            min value returned
        """
        query = 'sql %s format=json and stat=false "SELECT MIN(timestamp) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_min_timestamp.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_min_timestamp.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'MIN(value)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'MIN(timestamp)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregates_max_timestamp(self):
        """
        Test MAX aggregate against timestamp
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT MAV(value) FROM ping_sensor
        :assert:
            max value returned
        """
        query = 'sql %s format=json and stat=false "SELECT MAX(timestamp) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_max_timestamp.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_max_timestamp.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from 'MAX(timestamp)' (Error Code: %s | Error: %s)" % (
                            response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'MAX(timestamp)' (Error: %s)" % e)
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_group_by_uuid(self):
        """
        Test summary of data grouped by UUID
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                parentelement, MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value), COUNT(value)
            FROM
                ping_sensor
            GROUP BY
                parentelement
            ORDER BY
                parentelement DESC
        :assert:
           validate group by UUID
        """
        query = ('sql %s format=json and stat=false '
                 +'"SELECT parentelement, MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value),'
                 +' COUNT(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement DESC"')
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_group_by_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_group_by_uuid.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=expected_file, results=results) is True
                    # assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_group_by_string(self):
        """
        Test summary of data grouped by string
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                device_name, MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value), COUNT(value)
            FROM
                ping_sensor
            GROUP BY
                device_name
            ORDER BY
                device_name ASC
        :assert:
           validate group by string
        """
        query = ('sql %s format=json and stat=false '
                 +'"SELECT device_name, MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value), '
                 +'COUNT(value) FROM ping_sensor GROUP BY device_name ORDER BY device_name ASC"')
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_group_by_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_group_by_string.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=expected_file, results=results) is True
                    # assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_where_by_uuid(self):
        """
        Test summary of data WHERE against UUID
        :params:
            query:str - command to execute
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba'
            ORDER BY
                parentelement DESC
        :assert:
           validate group by UUID
        """
        sql_query = ("SELECT timestamp, value FROM ping_sensor "
                     +"WHERE parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba' ORDER BY timestamp DESC")
        query = 'sql %s format=json and stat=false "' + sql_query + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_uuid.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_where_by_string(self):
        """
        Test summary of data WHERE against string
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                device_name, MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value), COUNT(value)
            FROM
                ping_sensor
            WHERE
                device_name = 'ADVA FSP3000R7'
            ORDER BY
                device_name ASC
        :assert:
           validate group by string
        """
        sql_query = ("SELECT timestamp, value FROM ping_sensor "
                     + "WHERE device_name = 'ADVA FSP3000R7' ORDER BY timestamp ASC;")
        query = 'sql %s format=json and stat=false "' + sql_query  +'"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_string.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_where_by_timestamp_desc(self):
        """
        Validate basic WHERE condition
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00'
            ORDER BY
                timestamp DESC
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE "
               +"timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00' ORDER BY timestamp DESC")
        query = 'sql %s format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_timestamp_desc.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_timestamp_desc.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_where_by_timestamp_asc(self):
        """
        Validate basic WHERE condition
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00'
            ORDER BY
                timestamp ASC
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE "
               +"timestamp <= '2021-12-30 00:00:00' OR timestamp >= '2022-01-02 00:00:00' ORDER BY timestamp ASC")
        query = 'sql %s format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_timestamp_asc.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_timestamp_asc.rslts')

        if self.status is True:
            response = self.anylog_conn.get(headers=headers)
            if isinstance(response, dict):
                try:
                    results = response['Query']
                except Exception as e:
                    if 'err_code' in response and 'err_text' in response:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, response['err_code'], response['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, expected_file)
            else:
                pytest.fail(response)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    


