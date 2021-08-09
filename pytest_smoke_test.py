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
import support.file

CONFIG_FILE = 'config.ini' # Config file - located in AnyLog-API directory 

class TestBaseQueries: 
    """
    The following is intended to test a basic set of queries using anylog.ping_sensor sample data set
    :queries; 
        - SELECT COUNT(*) FROM ping_sensor
        - SELECT MIN(value) FROM ping_sensor
        - SELECT AVG(value) FROM ping_sensor
        - SELECT MAX(value) FROM ping_sensor
        - SELECT timestamp, value FROM ping_sensor ORDER BY timestamp LIMIT 1
        - SELECT timestamp, value FROM ping_sensor ORDER BY timestamp ASC LIMIT 1
            - SELECT MIN(timestamp) FROM ping_sensor
        - SELECT timestamp, value FROM ping_sensor ORDER BY timestamp DESC LIMIT 1
            - SELECT MAX(timestamp) FROM ping_sensor
    """
    def setup_class(self): 
        """
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
        self.config['expect_dir'] = os.path.expandvars(os.path.expanduser(self.config['expect_dir']))
        self.config['actual_dir'] = os.path.expandvars(os.path.expanduser(self.config['actual_dir']))
       
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
            self.status = rest.put_data.put_data(file_info='anylog.ping_sensor', conn=self.config['publish_conn'], auth=self.config['auth'], timeout=self.config['timeout'])
            time.sleep(10)
        elif self.config['insert'] == 'true':
            print('Faild to get status from: %s' % self.config['publish_conn'])
            status = False 

        if not rest.get.get_status(conn=self.config['query_conn'], auth=self.config['auth'], timeout=self.config['timeout']):
            print('Failed to get status from: %s' % self.config['query_conn'])
            status = False

        if status == False: 
            exit(1) 

    def test_put_data(self): 
        """
        PUT data in AnyLog 
        :param: 
            data:list - FROM put_data whether each insert was successful or not 
        :assert: 
            all data was inserted 
        """
        if self.config['insert'] == 'true': 
            assert all(True == i for i in self.status) == True 
        else: 
            pass 

    # Basic aggergate 
    def test_count(self): 
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

        assert int(output[0]['count(*)']) == 25862 

    def test_min(self): 
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

        assert float(output[0]['min(value)']) == 0.0

    def test_avg(self): 
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

        assert float(output[0]['avg(value)']) == 14.885159693759183

    def test_max(self): 
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

        assert float(output[0]['max(value)']) == 48.0

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
        
        assert output[0]['timestamp'] == '2021-07-21 22:16:24.652293'  
        assert float(output[0]['value']) == 2.0

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
        
        assert output[0]['timestamp'] == min_ts
        assert float(output[0]['value']) == 2.0

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
        
        assert output[0]['timestamp'] == max_ts
        assert float(output[0]['value']) == 34.0

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
        query = "select count(*) from ping_sensor where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z';"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        row_count = int(output[0]['count(*)'])

        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z' ORDER BY timestamp"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        assert len(output) == row_count 

        if len(output) == row_count: 
            file_name = 'base_queries_test_where_mid_day.json' 
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + '/%s' % file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + '/%s' % file_name, self.config['actual_dir'] + '/%s' % file_name) 

    def test_where_end_day(self):
        """
        Where condition is mid-day
        :params: 
            query:str - query to execute
            output - result from request 
        :assert:
            mid-day WHERE condition 
        """
        query = "select count(*) from ping_sensor where timestamp >= '2021-07-21T22:00:00Z' AND timestamp <= '2021-07-22T01:00:00Z';"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        row_count = int(output[0]['count(*)'])

        query = "SELECT timestamp, value FROM ping_sensor WHERE timestamp >= '2021-07-21T22:00:00Z' AND timestamp <= '2021-07-22T01:00:00Z' ORDER BY timestamp"
        output = rest.get.get_json(conn=self.config['query_conn'], query=self.cmd % query, remote=True, 
                auth=self.config['auth'], timeout=self.config['timeout']) 
        assert len(output) == row_count 

        if len(output) == row_count: 
            file_name = 'base_queries_test_where_end_day.json' 
            status = support.file.write_file(data=output, results_file=self.config['actual_dir'] + '/%s' % file_name)
            assert status == True
            assert filecmp.cmp(self.config['expect_dir'] + '/%s' % file_name, self.config['actual_dir'] + '/%s' % file_name) 
