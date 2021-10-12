import os
import sys
import time

import support.file as file
from support.anylog_api import AnyLogConnect
from support.rest import put_data, query_data
from support.deploy_anylog import deploy_anylog
from tests.conftest import option

slash_char = '/'
if sys.platform.startswith('win'):
    slash_char = '\\'

ROOT_DIR = os.getcwd() + slash_char + 'data' + slash_char

DATA_FILES=[
    ROOT_DIR + 'al_smoke_test.ping_sensor.0.20210721.json',
    ROOT_DIR + 'al_smoke_test.ping_sensor.0.20210722.json',
    ROOT_DIR + 'al_smoke_test.ping_sensor.0.20210723.json'
]

class TestAggregates:
    def setup_class(self):
        """
        The following class tests basic aggregate functions against float, timestamp and string type column respectively
            - COUNT
            - MIN
            - MAX
            - AVG
            - SUM
            - DISTINCT
            - COUNT DISTINCT
        :process:
            1. Extract config
            2. create node(s)
            3. Insert data
            4. connect to query node
            5. begin tests
        :local-params:
            config_file:str - full path of CONFIG_FILE
        :class-params:
            self.config_data:dict - values from config file
        """
        self.config_data = {}

        # Extract config file (user param: --config-file)
        config_file = os.path.expandvars(os.path.expanduser(option.config_file))

        # read config file -- if fails stops
        if os.path.isfile(config_file):
            self.config_data = file.read_config(config_file=config_file)
        if self.config_data == {}:
            exit(1)

        # deploy AnyLog instance(s) based on config file - note, nodes should be accessible via REST
        deploy_anylog(anylog_api_path=self.config_data['anylog_api'], anylog_api_config=self.config_data['anylog_api_info'])

        # Insert data
        if self.config_data['add_data'] is True:
            for file_name in DATA_FILES:
                put_data(node_config=self.config_data['nodes']['insert'], file_name=file_name)
            time.sleep(60)
            #self.config_data['add_data'] = False

        # connect to AnyLog instance
        self.conn = AnyLogConnect(conn=self.config_data['nodes']['query'], auth=(), timeout=30)

    def teardown_class(self):
        """
        clean AnyLog instance(s)
        """
        pass

    def test_count_all(self):
        """
        Check row count using '*'
        :assert:
            select count(*) as row_count from ping_sensor returns consistent results
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(*) as row_count from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['row_count']) == 25862, "Incorrect row count against '*'"

    def test_count_value(self):
        """
        Check row count using value column
        :assert:
            select count(value) as row_count from ping_sensor returns consistent results
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(value) as row_count from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['row_count']) == 25862, "Incorrect row count against '*'"

    def test_count_timestamp(self):
        """
        Check row count using timestamp
        :assert:
            select count(timestamp) as row_count from ping_sensor returns consistent results
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(timestamp) as row_count from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['row_count']) == 25862, "Incorrect row count against '*'"

    def test_count_string(self):
        """
        Check row count using timestamp
        :assert:
            select count(device_name) as row_count from ping_sensor returns consistent results
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(device_name) as row_count from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['row_count']) == 25862, "Incorrect row count against '*'"

    def test_min_value(self):
        """
        Check MIN aggregate against value column (float)
        :assert:
            select min(value) as min_value from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select min(value) as min_value from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert float(results[0]['min_value']) == 0.0

    def test_min_timestamp(self):
        """
        Check MIN aggregate against timestamp column
        :assert:
            select min(timestamp) as min_ts from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select min(timestamp) as min_ts from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert results[0]['min_ts'] == '2021-07-21 22:16:24.652293'

    def test_max_value(self):
        """
        Check MAX aggregate against value column (float)
        :assert:
            select max(value) as min_value from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select max(value) as max_value from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert float(results[0]['max_value']) == 48.0

    def test_max_timestamp(self):
        """
        Check MAX aggregate against timestamp column
        :assert:
            select max(timestamp) as max_ts from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select max(timestamp) as max_ts from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert results[0]['max_ts'] == '2021-07-23 01:59:58.768801'

    def test_avg_value(self):
        """
        Check AVG aggregate against  value column
        :assert:
            select avg(value) as avg_value from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select avg(value) as avg_value from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert float(results[0]['avg_value']) == 14.8851596937591834

    def test_sum_value(self):
        """
        Check SUM aggregate against  value column
        :assert:
            select sum(value) as sum_value from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select sum(value) as sum_value from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert float(results[0]['sum_value']) == 384960.0

    def test_distinct_value(self):
        """
        Check DISTINCT aggregate against value column
        :assert:
            distinct(value) is the same as `GROUP BY value`
        """
        query = 'sql al_smoke_test format=json and stat=false "select distinct(value) as distinct_value from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        distinct_results = []
        for row in results:
            distinct_results.append(row['distinct_value'])

        query = 'sql al_smoke_test format=json and stat=false "select value from ping_sensor group by value;"'
        results = query_data(conn=self.conn, command=query)
        group_results = []
        for row in results:
            group_results.append(row['value'])

        assert sorted(distinct_results) == sorted(group_results)

    def test_distinct_timestamp(self):
        """
        Check DISTINCT aggregate against timestamp column
        :assert:
            distinct(timestamp) is the same as `GROUP BY timestamp`
        """
        query = 'sql al_smoke_test format=json and stat=false "select distinct(timestamp) as distinct_ts from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        distinct_results = []
        for row in results:
            distinct_results.append(row['distinct_ts'])

        query = 'sql al_smoke_test format=json and stat=false "select timestamp from ping_sensor group by timestamp;"'
        results = query_data(conn=self.conn, command=query)
        group_results = []
        for row in results:
            group_results.append(row['timestamp'])

        assert sorted(distinct_results) == sorted(group_results)

    def test_distinct_string(self):
        """
        Check DISTINCT aggregate against device_name column
        :assert:
            distinct(device_name) is the same as `GROUP BY timestamp`
        """
        query = 'sql al_smoke_test format=json and stat=false "select distinct(device_name) as distinct_dn from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        distinct_results = []
        for row in results:
            distinct_results.append(row['distinct_dn'])

        query = 'sql al_smoke_test format=json and stat=false "select device_name from ping_sensor group by device_name;"'
        results = query_data(conn=self.conn, command=query)
        group_results = []
        for row in results:
            group_results.append(row['device_name'])

        assert sorted(distinct_results) == sorted(group_results)

    def test_count_distinct_value(self):
        """
        Check COUNT DISTINCT aggregate against value column
        :assert:
            select count(distinct(value)) as count_distinct from ping_sensor
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(distinct(value)) as count_distinct from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['count_distinct']) == 49

    def test_count_distinct_timestamp(self):
        """
        Check DISTINCT aggregate against timestamp column
        :assert:
            distinct(timestamp) is the same as `GROUP BY timestamp`
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(distinct(timestamp)) as count_distinct from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['count_distinct']) == 19161

    def test_count_distinct_string(self):
        """
        Check DISTINCT aggregate against device_name column
        :assert:
            distinct(device_name) is the same as `GROUP BY timestamp`
        """
        query = 'sql al_smoke_test format=json and stat=false "select count(distinct(device_name)) as count_distinct from ping_sensor;"'
        results = query_data(conn=self.conn, command=query)
        assert int(results[0]['count_distinct']) == 5
