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


class TestLitSanLeandroQueries:
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
    against datatypes:
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
                    pytest.fail("Failed to extract results from 'COUNT(*)' (Error: %s)" % e)
                else:
                    assert int(result) == 100
            else:
                pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

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
                                        query='SELECT DISTINCT(value) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results.append(row['distinct(value)'])
                except Exception as e:
                    pytest.fail("Failed to extract from data set DISTINCT(value) (Error: %s)" % e)
                else:
                    assert sorted(results) == ['0.02', '0.29', '0.31', '0.5', '0.63', '0.69', '0.71', '0.8', '0.83',
                                                '0.85', '0.88', '0.89', '0.94', '0.97', '1.14', '1.2', '1.27', '1.32',
                                                '1.33', '1.4', '1.64', '1.67', '1.68', '1.79', '1.81', '1.84', '1.87',
                                                '10.34', '10.81', '10.98', '11.1', '11.7', '12.29', '12.79', '13.58',
                                                '13.81', '14.11', '16.02', '19.2', '19.59', '19.96', '2.12', '2.13',
                                                '2.16', '2.29', '2.34', '2.45', '2.81', '2.91', '20.1', '20.3', '20.49',
                                                '22.12', '22.52', '23.64', '24.6', '25.92', '27.14', '28.62', '29.13',
                                                '3.38', '3.54', '3.64', '3.95', '3.96', '3.97', '31.14', '32.5',
                                                '33.31', '34.94', '34.98', '35.73', '38.59', '39.08', '39.86', '4.17',
                                                '4.25', '41.25', '43.54', '44.9', '44.92', '45.98', '5.28', '5.33',
                                                '6.01', '6.39', '6.45', '7.95', '8.11', '8.33', '8.42', '8.74', '8.79',
                                                '8.82', '9.17', '9.18', '9.3', '9.81']
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
                    pytest.fail("Failed to extract from data set DISTINCT(parentelement) (Error: %s)" % (column, e))
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
                    pytest.fail("Failed to extract from data set DISTINCT(device_name) (Error: %s)" % e)
                else:
                    assert results ==  ['VM Lit SL NMS', 'Catalyst 3500XL', 'ADVA FSP3000R7', 'Ubiquiti OLT',
                                        'GOOGLE_PING']
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_count_distinct_value(self):
        """
        Execute COUNT(COUNT) against float column
        :query:
            SELECT COUNT(DISTINCT(value)) FROM ping_sensor
        :assert:
            COUNT(DISTINCT) against a float column
        """
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(DISTINCT(value)) FROM ping_sensor',
                                        username=self.configs['rest_user'], password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    for row in output['Query']:
                        results = int(row['count(distinct(value))'])
                except Exception as e:
                    pytest.fail("Failed to extract from data set COUNT(DISTINCT(value)) (Error: %s)" % e)
                else:
                    assert results == 98
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
                    pytest.fail("Failed to extract from data set COUNT(DISTINCT(parentelement)) (Error: %s)" % e)
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
                    pytest.fail("Failed to extract from data set COUNT(DISTINCT(device_name) (Error: %s)" % e)
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
            SELECT MIN(value) FROM ping_sensor
            SELECT MAX(value) FROM ping_sensor
            SELECT AVG(value) FROM ping_sensor
            SELECT SUM(value) FROM ping_sensor
        :assert:
            correct results per qury
        """
        expected = {
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
                    result = float(output['Query'][0][query])
                except Exception as e:
                    pytest.fail('Failed to extract results from MIN(value) (Error: %s)' % e)
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
                    pytest.fail("Failed to extract data from query: '%s' (Error: %s)" % (query, e))
                else:
                    assert results == [{'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-12-09 01:36:25.319467',
                                        'max(timestamp)': '2022-01-28 09:08:20.155750', 'min(value)': '0.29',
                                        'max(value)': '3.97', 'avg(value)': '1.9552'},
                                       {'device_name': 'Catalyst 3500XL', 'min(timestamp)': '2021-12-04 10:00:36.357454',
                                        'max(timestamp)': '2022-01-26 19:13:51.238877', 'min(value)': '0.85',
                                        'max(value)': '43.54', 'avg(value)': '18.131052631578946'},
                                       {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-12-06 00:40:40.206160',
                                        'max(timestamp)': '2022-01-23 01:40:45.378731', 'min(value)': '2.12',
                                        'max(value)': '35.73', 'avg(value)': '18.000526315789475'},
                                       {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-12-04 18:10:07.271804',
                                        'max(timestamp)': '2022-01-26 18:54:48.389162', 'min(value)': '0.8',
                                        'max(value)': '45.98', 'avg(value)': '24.01625'},
                                       {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-12-07 04:52:55.247622',
                                        'max(timestamp)': '2022-01-29 11:51:14.136243', 'min(value)': '0.02',
                                        'max(value)': '10.34', 'avg(value)': '4.276666666666666'}]
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
                    pytest.fail("Failed to extract data from query: '%s' (Error: %s)" % (query, e))
                else:
                    assert results ==  [{'parentelement': '1ab3b14e-93b1-11e9-b465-d4856454f4ba',
                                         'min(timestamp)': '2021-12-07 04:52:55.247622',
                                         'max(timestamp)': '2022-01-29 11:51:14.136243',
                                         'min(value)': '0.02', 'max(value)': '10.34', 'avg(value)': '4.276666666666666'},
                                        {'parentelement': '62e71893-92e0-11e9-b465-d4856454f4ba',
                                         'min(timestamp)': '2021-12-09 01:36:25.319467',
                                         'max(timestamp)': '2022-01-28 09:08:20.155750',
                                         'min(value)': '0.29', 'max(value)': '3.97', 'avg(value)': '1.9552'},
                                        {'parentelement': '68ae8bef-92e1-11e9-b465-d4856454f4ba',
                                         'min(timestamp)': '2021-12-04 10:00:36.357454',
                                         'max(timestamp)': '2022-01-26 19:13:51.238877',
                                         'min(value)': '0.85', 'max(value)': '43.54', 'avg(value)': '18.131052631578946'},
                                        {'parentelement': 'd515dccb-58be-11ea-b46d-d4856454f4ba',
                                         'min(timestamp)': '2021-12-04 18:10:07.271804',
                                         'max(timestamp)': '2022-01-26 18:54:48.389162',
                                         'min(value)': '0.8', 'max(value)': '45.98', 'avg(value)': '24.01625'},
                                        {'parentelement': 'f0bd0832-a81e-11ea-b46d-d4856454f4ba',
                                         'min(timestamp)': '2021-12-06 00:40:40.206160',
                                         'max(timestamp)': '2022-01-23 01:40:45.378731',
                                         'min(value)': '2.12', 'max(value)': '35.73', 'avg(value)': '18.000526315789475'}
                                        ]
            else:
                pytest.fail(output)
        else:
            pytest.fail('Failed to validate connection to AnyLog')