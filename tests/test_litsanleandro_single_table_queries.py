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
import rest_get
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
        :steps:
            1. Validate EXPECTED_DIR exists
            2. create ACTUAL_DIR
            3. extract configs
            4. extract data to store
            5.
        """
        self.status, self.configs = pytest_setup_teardown.setup_code(table_name=['ping_sensor'], config_file=CONFIG_FILE,
                                                           data_dir=DATA_DIR, expected_dir=EXPECTED_DIR,
                                                           actual_dir=ACTUAL_DIR)
        if self.status is False:
            pytest.fail('Failed to get status against AnyLog node %s' % self.configs['conn'])

    def teardown_class(self):
        pytest_setup_teardown.teardown_code(actual_dir=ACTUAL_DIR)

    def test_row_count(self):
        """
        Check basic count(*)
        :query:
            SELECT COUNT(*) FROM ping_sensor
        :assert:
            all rows inserted
        """
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(*) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    result = output['Query'][0]['count(*)']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error Code: %s | Error: %s)" % (output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(*)' (Error: %s)" % e)
                else:
                    assert result == 100
            else:
                pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    @pytest.mark.skip('ORDER BY bug')
    def test_distinct_value(self):
        """
        Execute DISTINCT against float column
        :query:
            SELECT DISTINCT(value) FROM ping_sensor
        :assert:
            Distinct vales returned
        :note:
            There's currently an issue where the values returned are of type string rather than float
        """
        results = []
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT DISTINCT(value) as value FROM ping_sensor ORDER BY value',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results.append(row['distinct(value)'])
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error: %s)" % e)
                else:
                    print(results)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct_uuid(self):
        """
        Execute DISTINCT against an UUID column type
        :query:
            SELECT DISTINCT(parentelement) FROM ping_sensor
        :assert:
            unique UUID vales
        """
        results = []
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT DISTINCT(parentelement) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results.append(row['distinct(parentelement)'])
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error Code: %s | Error: %s)" % (output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
                else:
                    assert results == ['d515dccb-58be-11ea-b46d-d4856454f4ba', '62e71893-92e0-11e9-b465-d4856454f4ba',
                                       '68ae8bef-92e1-11e9-b465-d4856454f4ba', 'f0bd0832-a81e-11ea-b46d-d4856454f4ba',
                                       '1ab3b14e-93b1-11e9-b465-d4856454f4ba']
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct_string(self):
        """
        Execute DISTINCT against a string column
        :query:
            SELECT DISTINCT(device_name) FROM ping_sensor
        :assert:
            unique string values
        """
        results = []
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT DISTINCT(device_name) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results.append(row['distinct(device_name)'])
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error Code: %s | Error: %s)" % (output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'DISTINCT(device_name)' (Error: %s)" % e)
                else:
                    assert results ==  ['VM Lit SL NMS', 'Catalyst 3500XL', 'ADVA FSP3000R7', 'Ubiquiti OLT',
                                        'GOOGLE_PING']
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_count_distinct_uuid(self):
        """
        Execute COUNT(DISTINCT) against UUID
        :query:
            SELECT COUNT(DISTINCT(parentelement)) FROM ping_sensor
        :assert:
            COUNT(DISTINCT) of UUID vales
        """
        results = []
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(DISTINCT(parentelement)) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results=int(row['count(distinct(parentelement))'])
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(parentelement))' (Error Code: %s | Error: %s)" % (output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(parentelement))' (Error: %s)" % e)
                else:
                    assert results == 5
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_count_distinct_string(self):
        """
        Execute COUNT(DISTINCT) against string
        :query:
            SELECT DISTINCT(device_name) FROM ping_sensor
        :assert:
            unique count distinct of  string vales
        """
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(DISTINCT(device_name)) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results = int(row['count(distinct(device_name))'])
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(device_name))' (Error Code: %s | Error: %s)" % (output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from 'COUNT(DISTINCT(device_name))' (Error: %s)" % e)
                else:
                    assert results == 5
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_aggregate_values(self):
        """
        Validate min value
        :query:
            SELECT COUNT(DISTINCT(value)) FROM ping_sensor
            SELECT MIN(value) FROM ping_sensor
            SELECT MAX(value) FROM ping_sensor
            SELECT AVG(value) FROM ping_sensor
            SELECT SUM(value) FROM ping_sensor
        :assert:
            correct results per qury
        """
        expected = {
            'count(distinct(value))': 98,
            'min(value)': 0.02,
            'max(value)': 45.98,
            'avg(value)': 12.0945,
            'sum(value)': 1209.45
        }
        if self.status is True:
            for query in expected:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                            query='SELECT %s FROM ping_sensor' % query,
                                            username=self.configs['rest_user'], password=self.configs['rest_password'])
                try:
                    result = output['Query'][0][query]
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert result == expected[query]

    def test_summary_group_by_string(self):
        """
        Validate summary of data against string value
        :query:
             SELECT
                device_name, min(timestamp), max(timestamp), min(value), max(value), avg(value)
            FROM
                ping_sensor
            GROUP BY
                device_name
            ORDER BY
                device_name
        :aseert:
            summary of data based on device_name
        """
        query = "SELECT device_name, min(timestamp), max(timestamp), min(value), max(value), avg(value) FROM ping_sensor GROUP BY device_name ORDER BY device_name"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                            query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-12-09 01:36:25.319467',
                                        'max(timestamp)': '2022-01-28 09:08:20.155750', 'min(value)': 0.29,
                                        'max(value)': 3.97, 'avg(value)': 1.9552},
                                       {'device_name': 'Catalyst 3500XL',
                                        'min(timestamp)': '2021-12-04 10:00:36.357454',
                                        'max(timestamp)': '2022-01-26 19:13:51.238877', 'min(value)': 0.85,
                                        'max(value)': 43.54, 'avg(value)': 18.131052631578946},
                                       {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-12-06 00:40:40.206160',
                                        'max(timestamp)': '2022-01-23 01:40:45.378731', 'min(value)': 2.12,
                                        'max(value)': 35.73, 'avg(value)': 18.000526315789475},
                                       {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-12-04 18:10:07.271804',
                                        'max(timestamp)': '2022-01-26 18:54:48.389162', 'min(value)': 0.8,
                                        'max(value)': 45.98, 'avg(value)': 24.01625},
                                       {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-12-07 04:52:55.247622',
                                        'max(timestamp)': '2022-01-29 11:51:14.136243', 'min(value)': 0.02,
                                        'max(value)': 10.34, 'avg(value)': 4.276666666666666}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_summary_group_by_uuid(self):
        """
        Validate summary of data against UUID value
        :query:
             SELECT
                parentelement, min(timestamp), max(timestamp), min(value), max(value), avg(value)
            FROM
                ping_sensor
            GROUP BY
                parentelement
            ORDER BY
                parentelement
        :aseert:
            summary of data based on parentelement
        """
        query = "SELECT parentelement, min(timestamp), max(timestamp), min(value), max(value), avg(value) FROM ping_sensor GROUP BY parentelement ORDER BY parentelement"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'parentelement': '1ab3b14e-93b1-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2021-12-07 04:52:55.247622',
                                        'max(timestamp)': '2022-01-29 11:51:14.136243',
                                        'min(value)': 0.02, 'max(value)': 10.34, 'avg(value)': 4.276666666666666},
                                       {'parentelement': '62e71893-92e0-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2021-12-09 01:36:25.319467',
                                        'max(timestamp)': '2022-01-28 09:08:20.155750',
                                        'min(value)': 0.29, 'max(value)': 3.97, 'avg(value)': 1.9552},
                                       {'parentelement': '68ae8bef-92e1-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2021-12-04 10:00:36.357454',
                                        'max(timestamp)': '2022-01-26 19:13:51.238877',
                                        'min(value)': 0.85, 'max(value)': 43.54, 'avg(value)': 18.131052631578946},
                                       {'parentelement': 'd515dccb-58be-11ea-b46d-d4856454f4ba',
                                        'min(timestamp)': '2021-12-04 18:10:07.271804',
                                        'max(timestamp)': '2022-01-26 18:54:48.389162',
                                        'min(value)': 0.8, 'max(value)': 45.98, 'avg(value)': 24.01625},
                                       {'parentelement': 'f0bd0832-a81e-11ea-b46d-d4856454f4ba',
                                        'min(timestamp)': '2021-12-06 00:40:40.206160',
                                        'max(timestamp)': '2022-01-23 01:40:45.378731',
                                        'min(value)': 2.12, 'max(value)': 35.73, 'avg(value)': 18.000526315789475}]

            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_basic_where_timestamp_order_desc(self):
        """
        Validate basic WHERE condition
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00'
            ORDER BY timestamp DESC
        :assert:
            validate rows returned order by
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00' ORDER BY timestamp DESC"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                       {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29},
                                       {'timestamp': '2021-12-30 08:07:28.173834', 'value': 2.16}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_basic_where_timestamp_order_asc(self):
        """
        Validate basic WHERE condition
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00'
            ORDER BY timestamp ASC
        :assert:
            validate rows returned order by
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-12-30 00:00:00' AND timestamp <= '2022-01-02 00:00:00' ORDER BY timestamp ASC"
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-30 08:07:28.173834', 'value': 2.16},
                                       {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29},
                                       {'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_basic_where_timestamp_order_desc2(self):
        """
        Validate basic WHERE condition against timestamp
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp > '2021-12-20 00:00:00' AND timestamp < '2022-01-10 00:00:00'
            ORDER BY timestamp DESC
        :assert:
            1. validate content has been written to file
            2. validate content is consisent
        """
        query = ("SELECT timestamp, value FROM ping_sensor "
                +"WHERE timestamp > '2021-12-20 00:00:00' AND timestamp < '2022-01-10 00:00:00' "
                +"ORDER BY timestamp DESC")
        excepted_file = os.path.join(EXPECTED_DIR, 'test_basic_where_timestamp_order_desc2.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_basic_where_timestamp_order_desc2.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                   # assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_basic_where_timestamp_order_asc2(self):
        """
        Validate basic WHERE condition against timestamp
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                timestamp > '2021-12-20 00:00:00' AND timestamp < '2022-01-10 00:00:00'
            ORDER BY timestamp ASC
        :assert:
            1. validate content has been written to file
            2. validate content is consisent
        """
        query = ("SELECT timestamp, value FROM ping_sensor "
                +"WHERE timestamp > '2021-12-20 00:00:00' AND timestamp < '2022-01-10 00:00:00' "
                +"ORDER BY timestamp DESC")
        excepted_file = os.path.join(EXPECTED_DIR, 'test_basic_where_timestamp_order_asc2.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_basic_where_timestamp_order_asc2.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1minute(self):
        """
        Test increments by minute
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
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(minute, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
                 +"AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY MIN(timestamp) DESC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1minute.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1minute.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_30minute(self):
        """
        Test increments by 30 minutes
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
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(minute, 30, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
                 +"AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY min(timestamp) ASC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_30minute.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_30minute.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1hour(self):
        """
        Test increments by hour
        :query:
            SELECT
                increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp >= '2021-12-01 00:00:00' AND timestamp <= '2021-12-31 23:59:59'
            ORDER BY
                max(timestamp) DESC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
                 +"AVG(value) FROM ping_sensor WHERE timestamp >= '2021-12-01 00:00:00' AND "
                 +"timestamp <= '2021-12-31 23:59:59' ORDER BY max(timestamp) DESC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1hour.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1hour.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_12hour(self):
        """
        Test increments by hour
        :query:
            SELECT
                increments(hour, 12, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                timestamp <= '2022-12-15 00:00:00' OR timestamp >= '2022-01-15 23:59:59'
            ORDER BY
                max(timestamp) ASC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(hour, 12, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
                 +"AVG(value) FROM ping_sensor WHERE timestamp <= '2022-12-15 00:00:00' OR "
                  "timestamp >= '2022-01-15 23:59:59' ORDER BY max(timestamp) ASC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_12hour.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_12hour.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    @pytest.mark.skip('inconsistent results due to ORDER BY')
    def test_increments_day(self):
        """
        Test increments by day
        :query:
            SELECT
                increments(day, 1, timestamp), parentelement, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            GROUP BY
                parentelement
            ORDER BY
                parentelement, MIN(timestamp) DESC

        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(day, 1, timestamp), parentelement, MIN(timestamp), MAX(timestamp), MIN(value), "
                 +"MAX(value), AVG(value) FROM ping_sensor GROUP BY parentelement "
                 +"ORDER BY parentelement, MIN(timestamp) DESC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_day.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_day.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    @pytest.mark.skip('inconsistent results due to ORDER BY')
    def test_increments_5day(self):
        """
        Test increments by  5 day
        :query:
            SELECT
                increments(day, 5, timestamp), device_name, MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            GROUP BY
                device_name
            ORDER BY
                device_name, MIN(timestamp) ASC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(day, 5, timestamp), device_name, MIN(timestamp), MAX(timestamp), MIN(value), "
                 +"MAX(value), AVG(value) FROM ping_sensor GROUP BY device_name "
                 +"ORDER BY device_name, MIN(timestamp) ASC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_5day.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_5day.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_7day(self):
        """
        Test increments by 7 day
        :query:
            SELECT
                increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                device_name = 'Catalyst 3500XL'
            ORDER BY
                MIN(timestamp) ASC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
                 +"MAX(value), AVG(value) FROM ping_sensor WHERE device_name='Catalyst 3500XL' "
                 +"ORDER BY MAX(timestamp) ASC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_7day.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_7day.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_15day(self):
        """
        Test increments by 15 day
        :query:
            SELECT
                increments(day, 15, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            WHERE
                device_name = 'Catalyst 3500XL'
            ORDER BY
                MAX(timestamp) DESC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
                 +"MAX(value), AVG(value) FROM ping_sensor WHERE parentelement='62e71893-92e0-11e9-b465-d4856454f4ba' "
                 +"ORDER BY MAX(timestamp) DESC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_15day.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_15day.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_increments_1month(self):
        """
        Test increments by month
        :query:
            SELECT
                increments(month, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), AVG(value)
            FROM
                ping_sensor
            ORDER BY
                MAX(timestamp) DESC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        :note:
            once the date surpasses '2021-01-31' WHERE condition could just be "timestamp <= NOW()"
        """
        query = ("SELECT increments(month, 21, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
                 +"MAX(value), AVG(value) FROM ping_sensor ORDER BY MAX(timestamp) DESC")

        excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1month.json')
        actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1month.json')

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert file_io.write_file(file_name=actual_file, results=results) is True
                    assert filecmp.cmp(actual_file, excepted_file)
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1minute(self):
        """
        Test period by minute
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(minute, 1, NOW(), timestamp)
            ORDER BY
                timestamp DESC
        :assert:
            validate results are consistent
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 1, NOW(), timestamp) ORDER BY timestamp DESC"

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{"timestamp": "2022-01-04 21:08:33.187681", "value": 20.1}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_30minute(self):
        """
        Test period by 30 minute
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(minute, 30, NOW(), timestamp)
            ORDER BY
                timestamp DESC
        :assert:
            validate results are consistent
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 30, NOW(), timestamp) ORDER BY timestamp ASC"

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{"timestamp": "2022-01-04 21:08:33.187681", "value": 20.1}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1hour(self):
        """
        Test period by hour
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(hour, 1, '2022-01-01 00:00:00', timestamp)
        :assert:
            validate results are consistent
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE period(hour, 1, '2022-01-01 00:00:00', timestamp)"

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_12hour(self):
        """
        Test period by 12 hour
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(hour, 12, '2022-01-01 00:00:00', timestamp)
            ORDER BY
                timestamp DESC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE period(hour, 12, '2022-01-01 00:00:00', timestamp) ORDER BY timestamp DESC"

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                       {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_day(self):
        """
        Test period by 1 day
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(day, 1, '2021-12-31 23:59:59', timestamp)
            ORDER BY
                timestamp DESC
        :assert:
            1. content is writen to file
            2. validate results are consistent
        """
        query = "SELECT timestamp, value FROM ping_sensor WHERE period(day, 1, '2021-12-31 23:59:59', timestamp) ORDER BY timestamp DESC"

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                       {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29},
                                       {'timestamp': '2021-12-30 08:07:28.173834', 'value': 2.16}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_5day(self):
        """
        Test period by 5 day
        :query:
            SELECT
                device_name, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value)
            FROM
                ping_sensor
            WHERE
                period(day, 5, '2022-01-31 00:00:00', timestamp)
            GROUP BY
                device_name
            ORDER BY
                device_name ASC
        :assert:
            2. validate results are consistent
        """
        query = ("SELECT device_name, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value) "
                 +"FROM ping_sensor WHERE period(day, 5, '2022-01-31 00:00:00', timestamp) GROUP BY device_name "
                 +"ORDER BY device_name ASC")

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2022-01-25 09:15:16.139743',
                                        'max(timestamp)': '2022-01-28 09:08:20.155750', 'min(value)': 0.29,
                                        'max(value)': 3.64, 'avg(value)': 1.5675, 'count(value)': 4},
                                       {'device_name': 'Catalyst 3500XL',
                                        'min(timestamp)': '2022-01-26 19:13:51.238877',
                                        'max(timestamp)': '2022-01-26 19:13:51.238877',
                                        'min(value)': 28.62, 'max(value)': 28.62, 'avg(value)': 28.62,
                                        'count(value)': 1},
                                       {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2022-01-24 15:56:09.273241',
                                        'max(timestamp)': '2022-01-26 18:54:48.389162', 'min(value)': 8.42,
                                        'max(value)': 19.2, 'avg(value)': 13.81, 'count(value)': 2},
                                       {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2022-01-29 11:51:14.136243',
                                        'max(timestamp)': '2022-01-29 11:51:14.136243', 'min(value)': 4.17,
                                        'max(value)': 4.17, 'avg(value)': 4.17, 'count(value)': 1}]

            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_7day(self):
        """
        Test period by 7 day
        :query:
            SELECT
                parentelement, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value)
            FROM
                ping_sensor
            WHERE
                period(day, 7, '2022-01-31 00:00:00', timestamp)
            GROUP BY
                parentelement
            ORDER BY
                parentelement DESC
        :assert:
            2. validate results are consistent
        """
        query = ("SELECT parentelement, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value) "
                 +"FROM ping_sensor WHERE period(day, 7, '2022-01-31 00:00:00', timestamp) GROUP BY parentelement "
                 +"ORDER BY parentelement DESC")

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'parentelement': 'f0bd0832-a81e-11ea-b46d-d4856454f4ba',
                                        'min(timestamp)': '2022-01-23 01:40:45.378731',
                                        'max(timestamp)': '2022-01-23 01:40:45.378731', 'min(value)': 10.81,
                                        'max(value)': 10.81, 'avg(value)': 10.81, 'count(value)': 1},
                                       {'parentelement': 'd515dccb-58be-11ea-b46d-d4856454f4ba',
                                        'min(timestamp)': '2022-01-24 15:56:09.273241',
                                        'max(timestamp)': '2022-01-26 18:54:48.389162', 'min(value)': 8.42,
                                        'max(value)': 19.2, 'avg(value)': 13.81, 'count(value)': 2},
                                       {'parentelement': '68ae8bef-92e1-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2022-01-24 11:20:08.272811',
                                        'max(timestamp)': '2022-01-26 19:13:51.238877', 'min(value)': 0.85,
                                        'max(value)': 28.62, 'avg(value)': 14.735, 'count(value)': 2},
                                       {'parentelement': '62e71893-92e0-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2022-01-25 09:15:16.139743',
                                        'max(timestamp)': '2022-01-28 09:08:20.155750', 'min(value)': 0.29,
                                        'max(value)': 3.64, 'avg(value)': 1.5675, 'count(value)': 4},
                                       {'parentelement': '1ab3b14e-93b1-11e9-b465-d4856454f4ba',
                                        'min(timestamp)': '2022-01-23 21:34:15.293604',
                                        'max(timestamp)': '2022-01-29 11:51:14.136243', 'min(value)': 0.94,
                                        'max(value)': 4.17, 'avg(value)': 2.555, 'count(value)': 2}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_15day(self):
        """
        Test period by 15 day
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(day, 15, '2022-01-31 00:00:00', timestamp) and device_name = 'ADVA FSP3000R7'
            ORDER BY
                timestamp DESC
        :assert:
            2. validate results are consistent
        """
        query = ("SELECT timestamp, value "
                 +"FROM ping_sensor WHERE period(day, 15, '2022-01-31 00:00:00', timestamp) and "
                 +"device_name = 'ADVA FSP3000R7' ORDER BY timestamp DESC")

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2022-01-28 09:08:20.155750', 'value': 0.5},
                                       {'timestamp': '2022-01-27 16:17:02.263903', 'value': 1.84},
                                       {'timestamp': '2022-01-25 14:18:42.213060', 'value': 0.29},
                                       {'timestamp': '2022-01-25 09:15:16.139743', 'value': 3.64},
                                       {'timestamp': '2022-01-18 20:59:36.193183', 'value': 0.69}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_period_1month(self):
        """
        Test period by month
        :query:
            SELECT
                timestamp, value
            FROM
                ping_sensor
            WHERE
                period(month, 1, '2022-01-31 00:00:00', timestamp) and
                parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba'
            ORDER BY
                timestamp ASC
        :assert:
            2. validate results are consistent
        """
        query = ("SELECT timestamp, value "
                 +"FROM ping_sensor WHERE period(month, 1, '2022-01-31 00:00:00', timestamp) and "
                 +"parentelement = '62e71893-92e0-11e9-b465-d4856454f4ba' ORDER BY timestamp ASC")

        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'], query=query,
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    results = output['Query']
                except Exception as e:
                    if 'err_code' in output and 'err_text' in output:
                        pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, output['err_code'], output['err_text']))
                    else:
                        pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29},
                                       {'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                       {'timestamp': '2022-01-03 14:51:39.362594', 'value': 3.97},
                                       {'timestamp': '2022-01-09 14:29:19.303498', 'value': 1.67},
                                       {'timestamp': '2022-01-10 23:59:44.373455', 'value': 2.13},
                                       {'timestamp': '2022-01-11 14:45:12.129379', 'value': 0.83},
                                       {'timestamp': '2022-01-18 20:59:36.193183', 'value': 0.69},
                                       {'timestamp': '2022-01-25 09:15:16.139743', 'value': 3.64},
                                       {'timestamp': '2022-01-25 14:18:42.213060', 'value': 0.29},
                                       {'timestamp': '2022-01-27 16:17:02.263903', 'value': 1.84},
                                       {'timestamp': '2022-01-28 09:08:20.155750', 'value': 0.5}]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')