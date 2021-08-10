import filecmp
import json
import os 
import pytest
import sys
import time

rest_dir = os.path.expandvars(os.path.expanduser('$HOME/testing/rest'))
support_dir = os.path.expandvars(os.path.expanduser('$HOME/testing/support'))

sys.path.insert(0, rest_dir)
sys.path.insert(0, support_dir)

import rest.get
import rest.put_data
import support.convert 
import support.file

CONFIG_FILE = 'config.ini' # Config file - located in AnyLog-API directory 
if sys.platform.startswith('win'):
    slash_char = '\\'
else:
    slash_char = '/'

class TestBaseQueries: 
    def setup_class(self): 
        """
        The following is intended to test a basic set of queries using anylog.ping_sensor sample data set
        :process:
            1. prepare config
            2. create actual_dir if not exists 
            3. insert data 
        :param: 
            self.config:dict - config info
        """
        status = True
        self.cmd = 'sql anylog format=json and stat=false "%s"'
        # Read config
        self.config = support.file.read_config(CONFIG_FILE)
        self.config['expect_dir'] = os.path.expandvars(os.path.expanduser(self.config['expect_dir'])) + slash_char
        self.config['actual_dir'] = os.path.expandvars(os.path.expanduser(self.config['actual_dir'])) + slash_char
       
        # create actual_dir if not exists 
        if not os.path.isdir(self.config['actual_dir']): 
            try: 
                os.makedirs(self.config['actual_dir'])
            except FileExistsError:
                pass
            except Exception as e: 
                print('Failed to create dir: %s (Error: %s)' % (self.config['actual_dir'], e))
                exit(1)
        
        try: 
            self.config['auth'] = tuple(self.config['auth'])
        except: 
            self.config['auth'] = () 
        try: 
            self.config['timeout'] = int(self.config['timeout']) 
        except: 
            self.config['timeout'] = 30 

        # validate publish_conn & query_conn / insert data 
        if self.config['insert'] == 'true' and rest.get.get_status(conn=self.config['publish_conn'], auth=self.config['auth'], timeout=self.config['timeout']):
            rest.put_data.put_data(file_info='anylog.ping_sensor', conn=self.config['publish_conn'], auth=self.config['auth'], timeout=self.config['timeout'])
            time.sleep(10)
        elif self.config['insert'] == 'true':
            assert True == False, 'Faild to get status from: %s' % self.config['publish_conn']


        if not rest.get.get_status(conn=self.config['query_conn'], auth=self.config['auth'], timeout=self.config['timeout']):
            assert True == False, 'Failed to get status from: %s' % self.config['query_conn']

    # Basic aggregate
    def test_aggregates_count(self):
        """
        Validate row count 
        :param: 
            query:str - query to execute
            output - result from request 
        :assert: 
            row count 
        """
        query = 'SELECT COUNT(*) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 

        assert int(output[0]['count(*)']) == 25862, 'Failed Query: %s' % self.cmd % query

    def test_aggregates_min(self):
        """
        Validate min(value)
        :param: 
            query:str - query to execute
            output - result from request 
        :assert: 
            min value
        """
        query = 'SELECT MIN(value) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 

        assert float(output[0]['min(value)']) == 0.0, 'Failed Query: %s' % self.cmd % query

    def test_aggregates_avg(self):
        """
        Validate avg(value)
        :param: 
            output - result from request 
        :assert: 
            avg value
        """
        query = 'SELECT AVG(value) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 

        assert float(output[0]['avg(value)']) == 14.885159693759183, 'Failed Query: %s' % self.cmd % query

    def test_aggregates_max(self):
        """
        Validate max(value)
        :param: 
            query:str - query to execute
            output - result from request 
        :assert: 
            max value
        """
        query = 'SELECT MAX(value) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 

        assert float(output[0]['max(value)']) == 48.0, 'Failed Query: %s' % self.cmd % query

    # ORDER BY 
    def test_order_by(self): 
        """
        Validate ORDER BY with no conditions
        :params: 
            query:str - query to execute
            output - result from request
        :assert:
           ORDER BY without condition
        """
        query = 'SELECT timestamp, value FROM ping_sensor ORDER BY timestamp LIMIT 1' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout'])

        if self.config['convert_timezone'] == 'true':
            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=output[0]['timestamp']) == '2021-07-21 22:16:24.652293', 'Faild Query: %s' % self.cmd % query
        else:
            assert output[0]['timestamp'] == '2021-07-21 22:16:24.652293', 'Faild Query: %s' % self.cmd % query
        assert float(output[0]['value']) == 2.0, 'Failed Query: %s' % self.cmd % query

    def test_order_by_asc(self): 
        """
        Validate ORDER BY with ASC 
        :parrams: 
            query:str - query to execute
            min_ts:int - min timestamp
            output - result from request 
        :assert:
           ORDER BY ASC 
        """
        query = 'SELECT MIN(timestamp) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        min_ts = output[0]['min(timestamp)']  

        query = 'SELECT timestamp, value FROM ping_sensor ORDER BY timestamp ASC LIMIT 1' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        
        assert output[0]['timestamp'] == min_ts, 'Failed Query: %s' % self.cmd % query
        assert float(output[0]['value']) == 2.0, 'Failed Query: %s' % self.cmd % query

    def test_order_by_desc(self): 
        """
        Validate ORDER BY with DESC
        :parrams: 
            query:str - query to execute
            max_ts:int - max timestamp
            output - result from request 
        :assert:
           ORDER BY DESC
        """
        query = 'SELECT MAX(timestamp) FROM ping_sensor;' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        max_ts = output[0]['max(timestamp)']  

        query = 'SELECT timestamp, value FROM ping_sensor ORDER BY timestamp DESC LIMIT 1' 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        
        assert output[0]['timestamp'] == max_ts, 'Failed Query: %s' % self.cmd % query
        assert float(output[0]['value']) == 34.0, 'Failed Query: %s' % self.cmd % query

    # WHERE conditions
    def test_where_mid_day(self):
        """
        Where condition is mid-day
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            mid-day WHERE condition 
        """
        if self.config['convert_timezone'] == 'true':
            cmd = self.cmd.replace('format', 'timezone=utc and format')
        else:
            cmd = self.cmd

        query = "select count(*) from ping_sensor where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z';"
        output = rest.get.get_json(conn=self.config['query_conn'], query=cmd % query, remote=True,
                auth=self.config['auth'], timeout=self.config['timeout']) 
        row_count = int(output[0]['count(*)'])

        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z' ORDER BY timestamp"
        output = rest.get.get_json(conn=self.config['query_conn'], query=cmd % query, remote=True,
                auth=self.config['auth'], timeout=self.config['timeout']) 
        assert len(output) == row_count, 'Failed Query: %s' % cmd % query

        if len(output) == row_count: 
            file_name = 'base_queries_test_where_mid_day.json' 
            support.file.write_file(query=cmd % query, data=output, results_file=self.config['actual_dir'] + file_name)
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name), 'Failed Query: %s' % cmd % query

    def test_where_end_day(self):
        """
        Where condition is between 2 days
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            where condition between 2 days
        """
        if self.config['convert_timezone'] == 'true':
            cmd = self.cmd.replace('format', 'timezone=utc and format')
        else:
            cmd = self.cmd

        query = "select count(*) from ping_sensor where timestamp >= '2021-07-21T22:00:00Z' AND timestamp <= '2021-07-22T01:00:00Z';"
        output = rest.get.get_json(conn=self.config['query_conn'], query=cmd % query, remote=True,
                auth=self.config['auth'], timeout=self.config['timeout']) 
        row_count = int(output[0]['count(*)'])

        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-07-21T22:00:00Z' AND timestamp <= '2021-07-22T01:00:00Z' ORDER BY timestamp"
        output = rest.get.get_json(conn=self.config['query_conn'], query=cmd % query, remote=True,
                auth=self.config['auth'], timeout=self.config['timeout']) 
        assert len(output) == row_count, 'Failed Query: %s' % cmd % query
 

        if len(output) == row_count: 
            file_name = 'base_queries_test_where_end_day.json' 
            support.file.write_file(query=cmd % query, data=output, results_file=self.config['actual_dir'] + file_name)
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name), 'Failed Query: %s' % cmd % query

    def test_where_variable(self):
        """
        Where condition non-timestamp
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            where condition with non-timestamp
        """
        query = "select count(*) from ping_sensor where device_name='VM Lit SL NMS'"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        assert int(output[0]['count(*)']) == 5049, 'Failed Query: %s' % self.cmd % query 


        query = "select min(timestamp), max(timestamp), min(value), avg(value), max(value) from ping_sensor where device_name='VM Lit SL NMS'"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        if self.config['convert_timezone'] == 'true':
            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=output[0]['min(timestamp)']) == '2021-07-21 22:18:58.765161', 'Failed Query: %s' % self.cmd % query
            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=output[0]['max(timestamp)']) == '2021-07-23 01:59:14.737836', 'Failed Query: %s' % self.cmd % query
        else:
            assert output[0]['min(timestamp)'] == '2021-07-21 22:18:58.765161', 'Failed Query: %s' % self.cmd % query
            assert output[0]['max(timestamp)'] == '2021-07-23 01:59:14.737836', 'Failed Query: %s' % self.cmd % query
        assert float(output[0]['min(value)']) == 0.0, 'Failed Query: %s' % self.cmd % query
        assert float(output[0]['avg(value)']) == 4.956625074272133, 'Failed Query: %s' % self.cmd % query 
        assert float(output[0]['max(value)']) == 10.0, 'Failed Query: %s' % self.cmd % query 

    # Group by

    # GROUP by
    def test_group_by(self):
        """
        GROUP BY test
        :params; 
            expect_results:list - list of results
            query:str - query to execute
            output - result from request 
        :assert:
            GROUP BY
        """
        expect_results = [
            {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-07-21 22:18:58.765161', 'max(timestamp)': '2021-07-23 01:59:14.737836', 'min(value)': '0.0', 'avg(value)': '4.956625074272133', 'max(value)': '10.0'}, 
            {'device_name': 'Catalyst 3500XL', 'min(timestamp)': '2021-07-21 22:18:14.735669', 'max(timestamp)': '2021-07-23 01:57:24.649650', 'min(value)': '0.0', 'avg(value)': '24.203228315830415', 'max(value)': '48.0'}, 
            {'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-07-21 22:16:24.652293', 'max(timestamp)': '2021-07-23 01:59:36.746599', 'min(value)': '0.0', 'avg(value)': '1.4835597558574523', 'max(value)': '3.0'}, 
            {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-07-21 22:17:08.676983', 'max(timestamp)': '2021-07-23 01:59:58.768801', 'min(value)': '0.0', 'avg(value)': '24.112939416604338', 'max(value)': '48.0'}, 
            {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-07-21 22:20:26.830382', 'max(timestamp)': '2021-07-23 01:56:18.590668', 'min(value)': '2.0', 'avg(value)': '18.8768115942029', 'max(value)': '36.0'}
        ]

        query = "select device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value) from ping_sensor group by device_name"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        for row in output: 
            for result in expect_results:
                if row['device_name'] == result['device_name']: 
                    for key in row:
                        if 'timestamp' in key and self.config['convert_timezone'] == 'true':
                            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=row[key]) == result[key], 'Failed Query: %s' % self.cmd % query 
                        else: 
                            assert row[key] == result[key], 'Failed Query: %s' % self.cmd % query 

    # basic complex queries

    # queries containing where + group by
    def test_complex_query_mid_day(self):
        """
        Query with both WHERE & GROUP BY 
        :params: 
            expect_results:list - list of results
            query:str - query to execute
            output - result from request 
        :assert:
            WHERE with GROUP BY
        """
        expect_results = [
            {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-07-22 13:00:00.875931', 'max(timestamp)': '2021-07-22 15:59:58.232472', 'min(value)': '0.0', 'avg(value)': '4.891205802357208', 'max(value)': '10.0', 'count(*)': '1103'}, 
            {'device_name': 'Catalyst 3500XL', 'min(timestamp)': '2021-07-22 13:00:23.805597', 'max(timestamp)': '2021-07-22 15:59:52.075720', 'min(value)': '0.0', 'avg(value)': '24.580882352941178', 'max(value)': '48.0', 'count(*)': '1088'}, 
            {'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-07-22 13:00:02.790410', 'max(timestamp)': '2021-07-22 15:59:35.789967', 'min(value)': '0.0', 'avg(value)': '1.512987012987013', 'max(value)': '3.0', 'count(*)': '1078'}, 
            {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-07-22 13:00:16.804801', 'max(timestamp)': '2021-07-22 15:59:55.774716', 'min(value)': '0.0', 'avg(value)': '23.49682107175295', 'max(value)': '48.0', 'count(*)': '1101'}, 
            {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-07-22 13:00:05.972585', 'max(timestamp)': '2021-07-22 15:59:15.092310', 'min(value)': '2.0', 'avg(value)': '19.111737089201878', 'max(value)': '36.0', 'count(*)': '1065'}
        ] 

        query = "select device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z' group by device_name" 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        for row in output: 
            for result in expect_results:
                if row['device_name'] == result['device_name']: 
                    for key in row:
                        if 'timestamp' in key and self.config['convert_timezone'] == 'true':
                            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=row[key]) == result[key], 'Failed Query: %s' % self.cmd % query 
                        else: 
                            assert row[key] == result[key], 'Failed Query: %s' % self.cmd % query

    def test_complex_query_end_day(self): 
        """
        Query with both WHERE & GROUP BY 
        :params: 
            expect_results:list - list of results
            query:str - query to e}xecute
            output - result from request 
        :assert:
            WHERE with GROUP BY
        """
        expect_results = [
            {'device_name': 'VM Lit SL NMS', 'min(timestamp)': '2021-07-21 22:18:58.765161', 'max(timestamp)': '2021-07-22 00:59:55.437627', 'min(value)': '0.0', 'avg(value)': '4.8265963678968955', 'max(value)': '10.0', 'count(*)': '1707'}, 
            {'device_name': 'Catalyst 3500XL', 'min(timestamp)': '2021-07-21 22:18:14.735669', 'max(timestamp)': '2021-07-22 00:59:52.249602', 'min(value)': '0.0', 'avg(value)': '24.036557930258716', 'max(value)': '48.0', 'count(*)': '1778'}, 
            {'device_name': 'ADVA FSP3000R7', 'min(timestamp)': '2021-07-21 22:16:24.652293', 'max(timestamp)': '2021-07-22 00:59:58.709637', 'min(value)': '0.0', 'avg(value)': '1.4710312862108923', 'max(value)': '3.0', 'count(*)': '1726'}, 
            {'device_name': 'Ubiquiti OLT', 'min(timestamp)': '2021-07-21 22:17:08.676983', 'max(timestamp)': '2021-07-22 00:59:58.505636', 'min(value)': '0.0', 'avg(value)': '24.717091295116774', 'max(value)': '48.0', 'count(*)': '1884'}, 
            {'device_name': 'GOOGLE_PING', 'min(timestamp)': '2021-07-21 22:20:26.830382', 'max(timestamp)': '2021-07-22 00:59:59.289594', 'min(value)': '2.0', 'avg(value)': '18.915443335290664', 'max(value)': '36.0', 'count(*)': '1703'}
        ]

        query = "select device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-21T22:00:00Z' AND timestamp <= '2021-07-22T01:00:00Z' group by device_name" 
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        for row in output: 
            for result in expect_results:
                if row['device_name'] == result['device_name']: 
                    for key in row:
                        if 'timestamp' in key and self.config['convert_timezone'] == 'true':
                            assert support.convert.convert_timezone(query=self.cmd % query, timestamp=row[key]) == result[key], 'Failed Query: %s' % self.cmd % query 
                        else: 
                            assert row[key] == result[key], 'Failed Query: %s' % self.cmd % query 


    # increments
    def test_basic_increments_minute(self):
        """
        Test increments with minute interval
            - intervals: 1, 10, 30, 60
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            increments by minute
        """
        if self.config['convert_timezone'] == 'true':
            cmd = self.cmd.replace('format', 'timezone=utc and format')
        else:
            cmd = self.cmd
        for increment in [1, 10, 30, 60]:
            query = "select increments(minute, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=cmd % query, remote=True,
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_minute%s.json' % increment  
            support.file.write_file(query=query, data=output, results_file=self.config['actual_dir'] + file_name)
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name), 'Failed Query: %s' % cmd % query
    '''
    def test_basic_increments_hour(self):
        """
        Test increments with hour interval
            - intervals: 1, 6, 12, 24
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            increments by hour 
        """
        for increment in [1, 6, 12, 24]:
            query = "select increments(hour, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_hour%s.json' % increment  
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name) 

    def test_basic_increments_day(self):
        """
        Test increments with day interval
            - intervals: 1, 3, 5, 7 
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            increments by day 
        """
        for increment in [1, 3, 5, 7]:
            query = "select increments(day, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_day%s.json' % increment  
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name) 

    def test_basic_increments_group_by(self):
        """
        Test increments with group by
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            group by
        """
        for increment in ['minute', 'hour', 'day']:
            query = "select increments(%s, 1, timestamp), device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor group by device_name order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_group_by_%s.json' % increment  
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name) 
    
    def test_increments_where_mid_day(self): 
        """
        Test increments with where in mid-day
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            increments with in mid-day
        """
        for increment in ['minute', 'hour', 'day']:
            query = "select increments(%s, 1, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z' order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_where_mid_day_%s.json' % increment  
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name) 
 
    def test_increments_where_between_days(self): 
        """
        Test increments with where in mid-day
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            increments with in mid-day
        """
        for increment in ['minute', 'hour', 'day']:
            query = "select increments(%s, 1, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z' order by min(timestamp);" % increment
            output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                    auth=self.config['auth'], timeout=self.config['timeout']) 
            file_name = 'base_queries_test_increments_where_between_days_%s.json' % increment  
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + file_name, self.config['actual_dir'] + file_name) 
    ''' 
