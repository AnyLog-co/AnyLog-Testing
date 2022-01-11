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
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect', 'lit_san_leandro_multi_table_queries')
ACTUAL_DIR = os.path.join(ROOT_DIR, 'actual', 'lit_san_leandro_multi_table_queries')

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
            data_set_file = os.path.join(EXPECTED_DIR, 'data_set.txt')
            send_data.store_data(data_dir=DATA_DIR, data_set_file=data_set_file, send_type=self.configs['send'],
                                 dbms=self.configs['dbms'], anylog_conn=self.anylog_conn, topic=self.configs['topic'],
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT COUNT(*) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_row_count.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_row_count.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT MIN(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_min_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_min_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT AVG(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_avg_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_avg_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT MAX(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_max_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_max_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT SUM(value) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_sum_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_sum_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT DISTINCT(value) FROM ping_sensor ORDER BY value ASC"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_distinct_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT COUNT(DISTINCT(value)) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_count_distinct_value.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_count_distinct_value.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = ('sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT DISTINCT(parentelement) FROM ping_sensor '
                 +'ORDER BY parentelement ASC"')
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_distinct_uuid.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT DISTINCT(device_name) FROM ping_sensor ORDER BY device_name ASC"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_distinct_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_distinct_string.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT MIN(timestamp) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_min_timestamp.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_min_timestamp.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "SELECT MAX(timestamp) FROM ping_sensor"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_aggregates_max_timestamp.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_aggregates_max_timestamp.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = ('sql %s include=(percentagecpu_sensor) and format=json and stat=false '
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
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = ('sql %s include=(percentagecpu_sensor) and format=json and stat=false '
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
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql_query + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_uuid.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_uuid.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql_query  +'"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_string.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_string.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_timestamp_desc.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_timestamp_desc.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
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
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_by_timestamp_asc.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_by_timestamp_asc.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1minute(self):
        """
        Test increments by minute
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(minute, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp <= NOW() + 1 month
            ORDER BY
                min(timestamp) DESC
        :assert:
            increments for 1 minute
        """
        sql = ("SELECT increments(minute, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1minute.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1minute.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_30minute(self):
        """
        Test increments by 30 minute
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(minute, 30, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp <= NOW() + 1 month
            ORDER BY
                min(timestamp) ASC
        :assert:
            increments for 30 minute
        """
        sql = ("SELECT increments(minute, 30, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY MIN(timestamp) ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_30minute.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_30minute.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1hour(self):
        """
        Test increments by 1 hour
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp <= '2021-12-20 00:00:00' OR timestamp >= '2022-01-10 23:59:59'
            ORDER BY
                MAX(timestamp) DESC
        :assert:
            increments for 30 minute
        """
        sql = ("SELECT increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value) FROM ping_sensor WHERE timestamp <= '2021-12-20 00:00:00' OR "
               +"timestamp >= '2022-01-10 23:59:59' ORDER BY MAX(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1hour.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1hour.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_12hour(self):
        """
        Test increments by 12 hour
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(hour, 12, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp <= '2021-12-20 00:00:00' OR timestamp >= '2022-01-10 23:59:59'
            ORDER BY
                MAX(timestamp) ASC
        :assert:
            increments for 30 minute
        """
        sql = ("SELECT increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value) FROM ping_sensor WHERE timestamp <= '2021-12-20 00:00:00' OR "
               +"timestamp >= '2022-01-10 23:59:59' ORDER BY MAX(timestamp) ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_12hour.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_12hour.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1day(self):
        """
        Test increments by 1 day with group by UUID
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(day, 1, timestamp), parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value)
            FROM
                ping_sensor
            GROUP BY
                parentelement
            ORDER BY
                parentelement, MAX(timestamp) DESC
        :assert:
            increments for day with group by UUID
        """
        sql = ("SELECT increments(day, 1, timestamp), parentelement, MIN(timestamp), MAX(timestamp), MIN(value), "
               +"MAX(value), AVG(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement, "
               +"MAX(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_5day(self):
        """
        Test increments by 5 day with group by string
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(day, 5, timestamp), device_name, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value)
            FROM
                ping_sensor
            GROUP BY
                device_name
            ORDER BY
                device_name, MAX(timestamp) ASC
        :assert:
            increments for 5 day with group by string
        """
        sql = ("SELECT increments(day, 5, timestamp), device_name, MIN(timestamp), MAX(timestamp), MIN(value), "
               +"MAX(value), AVG(value) FROM ping_sensor GROUP BY device_name ORDER BY device_name, "
               +"MAX(timestamp) ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_5day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_5day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_7day(self):
        """
        Test increments by 7 day with WHERE by string
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value)
            FROM
                ping_sensor
            WHERE
                device_name = 'ADVA FSP3000R7'
            ORDER BY
                MIN(timestamp) ASC
        :assert:
            increments for 7 day with WHERE against string
        """
        sql = ("SELECT increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value) FROM ping_sensor WHERE device_name = 'ADVA FSP3000R7' ORDER BY MIN(timestamp) ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_7day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_7day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_15day(self):
        """
        Test increments by 15 day with WHERE by UUID
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(day, 15, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value)
            FROM
                ping_sensor
            WHERE
                parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba'
            ORDER BY
                MIN(timestamp) DESC
        :assert:
            increments for 15 day with WHERE against UUID
        """
        sql = ("SELECT increments(day, 15, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value) FROM ping_sensor WHERE parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba' "
               +"ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_15day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_15day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1month(self):
        """
        Test increments by month
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(month, 1, timestamp),  MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value), COUNT(value)
            FROM
                ping_sensor
            ORDER BY
                MIN(timestamp) DESC
        :assert:
            increments for month
        """
        sql = ("SELECT increments(day, 15, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value), COUNT(value) FROM ping_sensor ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1month.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1month.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1month(self):
        """
        Test increments by month
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(month, 1, timestamp),  MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value), COUNT(value)
            FROM
                ping_sensor
            ORDER BY
                MIN(timestamp) DESC
        :assert:
            increments for month
        """
        sql = ("SELECT increments(month, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value), COUNT(value) FROM ping_sensor ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1month.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1month.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1year(self):
        """
        Test increments by year
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(year, 1, timestamp),  MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value), COUNT(value)
            FROM
                ping_sensor
            ORDER BY
                MIN(timestamp) DESC
        :assert:
            increments for month
        """
        sql = ("SELECT increments(year, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value), COUNT(value) FROM ping_sensor ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1year.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1year.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1year(self):
        """
        Test increments by year
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                increments(year, 1, timestamp),  MIN(timestamp), MAX(timestamp), MIN(value), MAX(value),
                AVG(value), COUNT(value)
            FROM
                ping_sensor
            ORDER BY
                MIN(timestamp) DESC
        :assert:
            increments for month
        """
        sql = ("SELECT increments(year, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
               +"AVG(value), COUNT(value) FROM ping_sensor ORDER BY MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_increments_1year.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1year.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1minute(self):
        """
        Test period by minute
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
                period(minute, 1, '2022-02-05 18:27:43.748009', timestamp)
        :assert:
            list of timestamps and values
        """
        sql = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 1, '2022-02-05 18:27:43.748009', timestamp)"
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_1minute.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_1minute.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_30minute(self):
        """
        Test period by 30 minute
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
        :assert:
            list of timestamps and values
        """
        sql = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 30, '2022-02-05 18:27:43.748009', timestamp)"
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_30minute.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_30minute.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1hour(self):
        """
        Test period by hour
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
                period(hour, 1, '2022-01-07 06:57:46.380552', timestamp)
            ORDER BY
                timestamp DESC
        :assert:
            list of timestamps and values
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE period(hour, 1, '2022-01-07 06:57:46.380552', timestamp)"
               +" ORDER BY timestamp DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_1hour.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_1hour.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_12hour(self):
        """
        Test period by 12 hour
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
                period(hour, 12, '2022-01-14 18:22:47.229229', timestamp)
            ORDER BY
                timestamp ASC
        :assert:
            list of timestamps and values
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE period(hour, 12, '2022-01-14 18:22:47.229229', timestamp)"
               +" ORDER BY timestamp ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_12hour.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_12hour.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1day(self):
        """
        Test period by 1 day GROUP BY string
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(value)
            FROM
                ping_sensor
            WHERE
                period(day, 1, '2021-12-28 18:00:50.236529', timestamp)
            GROUP BY
                device_name
            ORDER BY
                device_name, MIN(timestamp) DESC
        :assert:
            aggregate summary over a day per string
        """
        sql = ("SELECT device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(value) "
               +"FROM ping_sensor WHERE period(day, 1, '2021-12-28 23:59:59', timestamp) GROUP BY device_name "
               +"ORDER BY device_name, MIN(timestamp) DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_1day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_1day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_5day(self):
        """
        Test period by 5 day GROUP BY UUID
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(value)
            FROM
                ping_sensor
            WHERE
                period(day, 5, '2021-12-28 18:00:50.236529', timestamp)
            GROUP BY
                parentelement
            ORDER BY
                parentelement, MIN(timestamp) DESC
        :assert:
            aggregate summary over a 5 day per uuid
        """
        sql = ("SELECT parentelement, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(value) "
               +"FROM ping_sensor WHERE period(day, 5, '2021-12-28 23:59:59', timestamp) GROUP BY parentelement "
               +"ORDER BY parentelement, MIN(timestamp) ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_5day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_5day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_7day(self):
        """
        Test period by 7 day WHERE BY UUID
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
                period(day, 7, '2021-12-28 18:00:50.236529', timestamp) AND p
                arentelement = '62e71893-92e0-11e9-b465-d4856454f4ba'
            ORDER BY
                timestmap DESC
        :assert:
            aggregate summary over a 7 day with WHERE against UUID
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE period(day, 7, '2022-01-28 23:59:59', timestamp) AND "
               +"parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba' ORDER BY timestamp DESC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_7day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_7day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_15day(self):
        """
        Test period by 15 day WHERE BY string
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
                period(day, 15, '2021-12-28 18:00:50.236529', timestamp) AND p
                device_name = 'ADVA FSP3000R7'
            ORDER BY
                timestmap ASC
        :assert:
            aggregate summary over a 15 day with WHERE against string
        """
        sql = ("SELECT timestamp, value FROM ping_sensor WHERE period(day, 15, '2022-01-28 23:59:59', timestamp) AND "
               +"device_name = 'ADVA FSP3000R7' ORDER BY timestamp ASC")
        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_15day.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_15day.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1month(self):
        """
        Test period by 1 month
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                 device_name, parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value),
                 COUNT(value)
            FROM
                ping_sensor
            WHERE
                period(month, 1, '2021-12-28 18:00:50.236529', timestamp)
            ORDER BY
                device_name, MIN(timestamp) ASC
        :assert:
            aggregate summary over a month
        """
        sql = ("SELECT device_name, parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value), "
              +"COUNT(value) FROM ping_sensor WHERE period(month, 1, '2022-01-28 23:59:59', timestamp) GROUP BY "
              +"device_name, parentelement ORDER BY device_name MIN(timestamp) ASC")

        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_1month.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_1month.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1year(self):
        """
        Test period by 1 month
        :params:
            headers:dict - REST headers
            results:list - results from query
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            SELECT
                 device_name, parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value),
                 COUNT(value)
            FROM
                ping_sensor
            WHERE
                period(year, 1, '2021-12-28 18:00:50.236529', timestamp)
            ORDER BY
                device_name, MIN(timestamp) DESC
        :assert:
            aggregate summary over a year
        """
        sql = ("SELECT device_name, parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value), "
              +"COUNT(value) FROM ping_sensor WHERE period(year, 1, '2022-01-28 23:59:59', timestamp) GROUP BY "
              +"device_name, parentelement ORDER BY device_name MIN(timestamp) DESC")

        query = 'sql %s include=(percentagecpu_sensor) and format=json and stat=false "' + sql + '"'
        headers = {
            'command': query % self.configs['dbms'],
            'User-Agent': 'AnyLog/1.23',
            'destination': 'network'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_period_1year.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_period_1year.rslts')

        if self.status is True:
            pytest_setup_teardown.execute_test(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                               expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')
