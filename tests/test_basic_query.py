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
            if 'ping_sensor' in fn:
                file_name = os.path.join(DATA_DIR, fn)
                self.payloads += file_io.read_file(file_name=file_name)

        if self.configs['insert'] == 'true':
            send_data.store_payloads(payloads=self.payloads, configs=self.configs)

        self.data_sets = {
            'value': support.extract_values(payloads=self.payloads, values_column='value'), # float
            'timestamp': support.extract_values(payloads=self.payloads, values_column='timestamp'), # timestamp
            'parentelement': support.extract_values(payloads=self.payloads, values_column='parentelement'), # UUID
            'device_name': support.extract_values(payloads=self.payloads, values_column='device_name') # string
        }

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
                    assert int(result) == len(self.payloads)
            else:
                pytest.fail(output.text)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_distinct(self):
        """
        Execute DISTINCT(%s)
        :data types:
            - float
            - UUID
            - string
        :query:
            SELECT DISTINCT(%s) FROM ping_sensor
        :assert:
            DISTINCT(%s) == set(self.data_sets[%s])
        """
        if self.status is True:
            for column in ['value', 'parentelement', 'device_name']:
                data_sets = []
                output = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                            query='SELECT DISTINCT(%s) FROM ping_sensor' % column,
                                            username=self.configs['rest_user'],
                                            password=self.configs['rest_password'])
                if isinstance(output, dict):
                    try:
                        for row in output["Query"]:
                            value = row['distinct(%s)' % column]
                            if column == 'value':
                                value = float(value)
                            if value not in data_sets:
                                data_sets.append(value)
                    except Exception as e:
                        pytest.fail("Failed to extract from data set DISTINCT(%s) (Error: %s)" % (column, e))
                    else:
                        assert sorted(set(data_sets)) == sorted(set(self.data_sets[column]))
                else:
                    pytest.fail(output.text)
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

