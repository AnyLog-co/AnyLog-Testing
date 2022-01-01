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


class TestBasicQueries:
    """
    The following tests basic aggregate functions without any conditions against the following data types:
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
            if 'litsanleandro' in fn:
                file_name = os.path.join(DATA_DIR, fn)
                self.payloads += file_io.read_file(file_name=file_name, dbms=self.configs['dbms'])

        if self.configs['insert'] == 'true':
            send_data.store_payloads(payloads=self.payloads, configs=self.configs)

        self.status = rest_get.get_status(conn=self.configs['conn'], username=self.configs['rest_user'],
                                          password=self.configs['rest_password'])

    def test_row_count(self):
        """
        Execute COUNT(*)
        :query:
            SELECT COUNT(*) FROM ping_sensor
        :assert:
            COUNT(*) == len(self.payloads)
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

    def test_distinct_value_asc(self):
        """
        Execute DISTINCT(%s)
        :query:
            SELECT DISTINCT(%s) FROM ping_sensor
        :assert:
            DISTINCT(%s) == set(self.data_sets[%s])
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
                    pytest.fail("Failed to extract from data set DISTINCT(%s) (Error: %s)" % (column, e))
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

    def test_count_distinct(self):
        """
        Execute COUNT(DISTINCT(%s))
        :data types:
            - float
            - UUID
            - string
        :query:
            SELECT COUNT(DISTINCT(%s)) FROM ping_sensor
        :assert:
            COUNT(DISTINCT(%s)) == len(set(self.data_sets[%s]))
        """
        if self.status is True:
            for column in ['value', 'parentelement', 'device_name']:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                            query='SELECT COUNT(DISTINCT(%s)) FROM ping_sensor' % column,
                                            username=self.configs['rest_user'],
                                            password=self.configs['rest_password'])
                if isinstance(output, dict):
                    try:
                        result = int(output['Query'][0]['count(distinct(%s))' % column])
                    except Exception as e:
                        pytest.fail("Failed to extract from data set COUNT(DISTINCT(%s)) (Error: %s)" % (column, e))
                    else:
                        assert  result == len(set(self.data_sets[column]))
                else:
                    pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_min(self):
        """
        Execute MIN(%s)
        :data types:
            - float
            - timestamp
        :query:
            SELECT MIN(%s) FROM ping_sensor
        :assert:
            MIN(%s) == min(self.data_sets[%s])
        """
        if self.status is True:
            for column in ['value', 'timestamp']:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                            query='SELECT MIN(%s) FROM ping_sensor' % column,
                                            username=self.configs['rest_user'],
                                            password=self.configs['rest_password'])
                if isinstance(output, dict):
                    try:
                        if column == 'value':
                            result = float(output['Query'][0]['min(%s)' % column])
                        else:
                            result = output['Query'][0]['min(%s)' % column]
                    except Exception as e:
                        pytest.fail("Failed to extract from data set MIN(%s) (Error: %s)" % (column, e))
                    else:
                        if column == 'timestamp':
                            assert result == datetime.datetime.strftime(min(self.data_sets[column]), '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            assert result == min(self.data_sets[column])
                else:
                    pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_max(self):
        """
        Execute MAX(%s)
        :data types:
            - float
            - timestamp
        :query:
            SELECT MAX(%s) FROM ping_sensor
        :assert:
            MAX(%s) == max(self.data_sets[%s])
        """
        if self.status is True:
            for column in ['value', 'timestamp']:
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                            query='SELECT MAX(%s) FROM ping_sensor' % column,
                                            username=self.configs['rest_user'],
                                            password=self.configs['rest_password'])
                if isinstance(output, dict):
                    try:
                        if column == 'value':
                            result = float(output['Query'][0]['max(%s)' % column])
                        else:
                            result = output['Query'][0]['max(%s)' % column]
                    except Exception as e:
                        pytest.fail("Failed to extract from data set MAX(%s) (Error: %s)" % (column, e))
                    else:
                        if column == 'timestamp':
                            assert result == datetime.datetime.strftime(max(self.data_sets[column]), '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            assert result == max(self.data_sets[column])
                else:
                    pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_sum(self):
        """
        Execute SUM(value)
        :data types:
            - float
        :query:
            SELECT SUM(value) FROM ping_sensor
        :assert:
            SUM(value) == sum(self.data_sets[value])
        """
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT SUM(value) FROM ping_sensor',
                                        username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    result = float(output['Query'][0]['sum(value)'])
                except Exception as e:
                    pytest.fail("Failed to extract from data set SUM(value) (Error: %s)" % e)
                else:
                    assert result == round(sum(self.data_sets['value']), 2)
            else:
                pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_avg(self):
        """
        Execute AVG(value)
        :data types:
            - float
        :query:
            SELECT AVG(value) FROM ping_sensor
        :assert:
            AVG(value) == sum(self.data_sets[value])/len(self.data_sets[value])
        """
        if self.status is True:
            output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT AVG(value) FROM ping_sensor',
                                        username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(output, dict):
                try:
                    result = float(output['Query'][0]['avg(value)'])
                except Exception as e:
                    pytest.fail("Failed to extract from data set AVG(value) (Error: %s)" % e)
                else:
                    assert result == round(sum(self.data_sets['value'])/len(self.data_sets['value']), 4)
            else:
                pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

