import collections
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
    """The following tests basic aggregate functions without any conditions
    * count
    * count distinct
    * distinct
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

        self.values = support.extract_values(payloads=self.payloads, values_column='value')

        self.status = rest_get.get_status(conn=self.configs['conn'], username=self.configs['rest_user'],
                                          password=self.configs['rest_password'])

    def test_row_count(self):
        """
        Validate the all count distinct
        :query:
            SELECT count(*) FROM ping_sensor
        :assert:
            query returns the same number of values as the number of rows inserted
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(*) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['count(*)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'COUNT(*)' (Error: %s)" % e)
            else:
                assert result == len(self.values)

    def test_distinct(self):
        """
        Validate the all content has been pushed into AnyLog
        :query:
            SELECT count(*) FROM ping_sensor
        :assert:
            query returns the same number of values as the number of rows inserted
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT DISTINCT(value) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['distinct(value)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'COUNT(DISTINCT(value))' (Error: %s)" % e)
            else:
                assert result == set(self.values)

    def test_count_distinct(self):
        """
        Validate the all content has been pushed into AnyLog
        :query:
            SELECT count(*) FROM ping_sensor
        :assert:
            query returns the same number of values as the number of rows inserted
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT COUNT(DISTINCT(value)) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['count(distinct(value))']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'COUNT(DISTINCT(value))' (Error: %s)" % e)
            else:
                assert result == collections.Counter(self.values)

    def test_min_value(self):
        """
        Validate min value
        :query:
            SELECT min(value) FROM ping_sensor
        :assert:
            query returns the correct min value
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT MIN(value) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['min(value)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'MIN(*)' (Error: %s)" % e)
            else:
                assert result == min(self.values)

    def test_max_value(self):
        """
        Validate max value
        :query:
            SELECT max(value) FROM ping_sensor
        :assert:
            query returns the correct min value
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT MAX(value) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['max(value)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'max(value)' (Error: %s)" % e)
            else:
                assert result == max(self.values)

    def test_sum_value(self):
        """
        Validate sum value
        :query:
            SELECT sum(value) FROM ping_sensor
        :assert:
            query returns the correct sum value
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT SUM(value) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['sum(value)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'SUM(value)' (Error: %s)" % e)
            else:
                assert result == sum(self.values)

    def test_avg_value(self):
        """
        Validate sum value
        :query:
            SELECT sum(value) FROM ping_sensor
        :assert:
            query returns the correct sum value
        """
        if self.status is True:
            result = rest_get.get_basic(conn=self.configs['conn'], dbms=self.configs['dbms'],
                                        query='SELECT AVG(value) FROM ping_sensor', username=self.configs['rest_user'],
                                        password=self.configs['rest_password'])
            if isinstance(result, dict):
                try:
                    result = result["Query"][0]['avg(value)']
                except Exception as e:
                    pytest.fail("Failed to extract results for 'AVG(value)' (Error: %s)" % e)
            else:
                assert result == sum(self.values)/len(self.values)

