import filecmp
import json
import os 
import pytest
import sys
import time

support_dir = os.path.expandvars(os.path.expanduser('$HOME/AnyLog-API/support'))
sys.path.insert(0, support_dir)

from support.config import read_config
from support.put_data import put_data
from support.rest_query import rest_query

CONFIG_FILE = 'config.ini' # Config file - located in AnyLog-API directory 

def print_exception(step:str, conn:str, query:str, error:str)->bool:
    """
    Print error message
    :args: 
        step:str - whether the failure occured on: 
            -- request (get) 
            -- network failure (network) 
            -- data extraction (json)
        conn:str - REST connection to AnyLog
        query:str - Query executed
        error:str - error from exception
    """
    if step.lower() == 'get': 
        print('Failed to execute GET on %s (Error: %s)\n\tQuery: %s\n' % (conn, error, query))
    elif step.lower() == 'network': 
        print('Failed to execute GET on %s due to network error: %s.\n\tQuery: %s\n' % (conn, error, query))
    elif step.lower() == 'json': 
        print('Failed to extract data from %s (Error: %s).\n\tQuery: %s\n' % (conn, error, query))


class TestSmoke: 
    def setup_class(self): 
        """
        The following is intended to act as the smoke-test for AnyLog
        :process:
            1. prepare config
            2. create actual_dir if not exists 
            3. insert data 
        :param: 
            self.config:dict - config info 
        """ 
        # Read config
        self.config = read_config(CONFIG_FILE)
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
        
        # insert data 
        if self.config['insert'] == 'true': 
            self.status = put_data(self.config['publish_conn'])
            time.sleep(10)

    def __get_request(self, query:str)->list: 
        """
        Method to execute query
        :args:
            query:str - query to execute
        :params: 
            output:list - data extracted
        :return: 
            output
        """
        output = [] 
        try: 
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

    def test_status(self): 
        """
        Valide status for publisher & query connections 
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. assert node is running 
        """
        for ip in ['publish_conn', 'query_conn']: 
            r, error = rest_query(self.config[ip], command='get status')
            if error != None: 
                print('Failed to GET status (Error: %s)' % error) 
                assert False 
            assert r.status_code == 200
            assert 'running' in r.text

    # Basic aggergate 
    def test_count(self): 
        """
        Validate row count 
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. row count 
        """
        r, error = rest_query(
            self.config['query_conn'], 
            command='sql anylog format=json and stat=False "select count(*) from ping_sensor;"', 
            query=True
        )
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        assert int(r.json()['Query'][0]['count(*)']) == 25862 

    def test_min(self): 
        """
        Validate min value
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. min value
        """
        r, error = rest_query(
            self.config['query_conn'], 
            command='sql anylog format=json and stat=False "select min(value) from ping_sensor;"', 
            query=True
        )
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        assert float(r.json()['Query'][0]['min(value)']) == 0.0 

    def test_avg(self): 
        """
        Validate avg value
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. AVG value 
        """
        r, error = rest_query(
            self.config['query_conn'], 
            command='sql anylog format=json and stat=False "select avg(value) from ping_sensor;"', 
            query=True
        )
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        assert float(r.json()['Query'][0]['avg(value)']) == 14.885159693759183

    def test_max(self): 
        """
        Validate max value
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. max value 
        """
        r, error = rest_query(
            self.config['query_conn'], 
            command='sql anylog format=json and stat=False "select max(value) from ping_sensor;"', 
            query=True
        )
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200
        assert float(r.json()['Query'][0]['max(value)']) == 48.0

    # ORDER BY & WHERE Conditions 
    def test_order_by_asc(self): 
        """
        Validate ORDER BY ASC
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. validate min timestamp == first timestamp with ASC 
            4. validate value is consistant 
        """
        cmd = 'sql anylog format=json and stat=False "select min(timestamp) from ping_sensor"'
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        min_timestamp = r.json()['Query'][0]['min(timestamp)']

        cmd = 'sql anylog format=json and stat=False "select timestamp, value from ping_sensor order by timestamp asc limit 1"' 
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        assert r.json()['Query'][0]['timestamp'] == min_timestamp
        assert float(r.json()['Query'][0]['value']) == 2.0

    def test_order_by_desc(self): 
        """
        Validate ORDER BY DESC
        :param: 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. validate max timestamp == first timestamp with DESC 
            4. validate value is consistant 
        """
        cmd = 'sql anylog format=json and stat=False "select max(timestamp) from ping_sensor"'
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        max_timestamp = r.json()['Query'][0]['max(timestamp)']

        cmd = 'sql anylog format=json and stat=False "select timestamp, value from ping_sensor order by timestamp desc limit 1"' 
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        assert r.json()['Query'][0]['timestamp'] == max_timestamp
        assert float(r.json()['Query'][0]['value']) == 34.0 

    def test_mid_day_where(self): 
        """
        Validate basic where condition (with order by) 
        :param: 
            results_file:str - file name containing results 
            query:str - SQL query to execute
            cmd:str - full AnyLog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. all values within range returned 
        """
        where_condition = " where timestamp >= '2021-07-22T13:00:00Z' AND timestamp <= '2021-07-22T16:00:00Z'" 
        query = "select count(*) from ping_sensor %s;" % where_condition  

        cmd = 'sql anylog format=json and stat=False  "%s"' % query
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 

        assert r.status_code == 200 
        try: 
            row_count = int(r.json()['Query'][0]['count(*)'])
        except Exception as e: 
            print('Failed to extract JSON data from query: %s (Error: %s)' % (cmd, e))
            assert False 

        query = "select timestamp, value from ping_sensor %s order by timestamp;" % where_condition

        assert len(r.json()['Query']) == row_count 
        results_file = 'test_mid_day_where.json'
        """
        try: 
            with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                for row in r.json()['Query']: 
                    f.write(json.dumps(row) + '\n')
        except: 
            assert False
        else: 
            assert filecmp.cmp(
                self.config['expect_dir'] + '/%s' % results_file, 
                self.config['actual_dir'] + '/%s' % results_file
            ) == True
        """
    '''
    # Group by 
    def test_basic_group_by(self): 
        """
        Validate basic GROUP BY 
        :param: 
            results_file:str - file name containing results 
            query:str - SQL query to execute
            cmd:str - full AnyLog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. group by function 
        """
        results_file = 'test_basic_group_by.json'
        query = "select device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor group by device_name"
        cmd = 'sql anylog format=json and stat=False "%s"' % query
        r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
        if error != None: 
            print('Failed to GET row count (Error: %s)' % e)
            assert False 
        assert r.status_code == 200 
        try: 
            with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                for row in r.json()['Query']: 
                    f.write(json.dumps(row) + '\n')
        except: 
            assert False
        else: 
            assert filecmp.cmp(
                self.config['expect_dir'] + '/%s' % results_file, 
                self.config['actual_dir'] + '/%s' % results_file
            ) == True

    # increments 
    def test_basic_increments_minute(self): 
        """
        test increments for minute interval 
            - intervals: 1, 10, 30, 60 
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval
        """
        for value in [1, 10, 30, 60]: 
            results_file = 'test_basic_increments_minute%s.json' % value 
            query = "select increments(minute, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_basic_increments_hour(self): 
        """
        test increments for hour interval 
            - intervals: 1, 6, 12, 23 
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval
        """
        for value in [1, 6, 12, 24]: 
            results_file = 'test_basic_increments_hour%s.json' % value 
            query = "select increments(hour, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_basic_increments_day(self): 
        """
        test increments for day interval 
            - intervals: 1, 3, 5, 7 
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval
        """
        for value in [1, 3, 5, 7]: 
            results_file = 'test_basic_increments_day%s.json' % value 
            query = "select increments(day, %s, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor order by min(timestamp);" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True


    def test_increments_group_by(self):
        """
        test increments with group by
           - interval period: minute, hour, day
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval period using group by 
        """
        for period in ['minute', 'hour', 'day']: 
            results_file = 'test_basic_increments_%s_group_by.json' % period 
            query = "select increments(%s, 1, timestamp), device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor group by device_name order by min(timestamp);" % period 
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_increments_where(self):
        """
        test increments with where condition
            - interval period: minute, hour, day
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval period using where condition 
        """
        for period in ['minute', 'hour', 'day']:
            results_file = 'test_basic_increments_%s_where.json' % period 
            query = "select increments(%s, 1, timestamp), min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-24T00:00:00Z' and timestamp <= '2021-07-24T23:59:59Z' order by min(timestamp);" % period
            if period == 'day': 
                query = query.replace("timestamp >= '2021-07-24T00:00:00Z' and timestamp <= '2021-07-24T23:59:59Z'", "timestamp >= '2021-07-23T00:00:00Z' and timestamp <= '2021-07-25T23:59:59Z'")
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('failed to get row count (error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_increments_both(self):
        """
        test increments with where & group by condition
            - interval period: minute, hour, day
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. increments are valid for each interval period using both using and group by conditions
        """
        for period in ['hour', 'minute', 'day']: 
            results_file = 'test_basic_increments_%s_both.json' % period
            query = "select increments(%s, 1, timestamp), device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where timestamp >= '2021-07-24T00:00:00Z' and timestamp <= '2021-07-24T23:59:59Z' group by device_name order by min(timestamp);" % period
            if period == 'day': 
                query = query.replace("timestamp >= '2021-07-24T00:00:00Z' and timestamp <= '2021-07-24T23:59:59Z'", "timestamp >= '2021-07-23T00:00:00Z' and timestamp <= '2021-07-25T23:59:59Z'")
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('failed to get row count (error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    # period 
    def test_basic_period_minute(self): 
        """
        test period for minute
            - intervals: 1, 10, 30, 60
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. period are valid for each interval 
        """
        for value in [1, 10, 30, 60]: 
            results_file = 'test_basic_period_minute%s.json' % value 
            query = "select timestamp, value from ping_sensor where period(minute, %s, now(), timestamp) order by timestamp;" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_basic_period_hour(self): 
        """
        test period for hour
            - intervals: 1, 6, 12, 24
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. period are valid for each interval 
        """
        for value in [1, 6, 12, 24]: 
            results_file = 'test_basic_period_hour%s.json' % value 
            query = "select timestamp, value from ping_sensor where period(hour, %s, now(), timestamp) order by timestamp;" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_basic_period_day(self): 
        """
        test period for day
            - intervals: 1, 3, 5, 7
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. period are valid for each interval 
        """
        for value in [1, 3, 5, 7]: 
            results_file = 'test_basic_period_day%s.json' % value 
            query = "select timestamp, value from ping_sensor where period(day, %s, now(), timestamp) order by timestamp;" % value
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % e)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True

    def test_basic_period_group_by(self): 
        """
        test period with group by
            - period: minute, hour, day
        :param: 
            results_file:str - file name containing results 
            query:str - sql query to execute
            cmd:str - full anylog command in header 
            r - result from request 
            error:str - error if request fails 
        :assert: 
            1. assert request didn't fail 
            2. assert status_code  == 200 
            3. period are valid for each interval 
        """
        for period in ['minute', 'hour', 'day']:
            results_file = 'test_basic_period_%s_group_by.json' % period
            query = "select device_name, min(timestamp), max(timestamp), min(value), avg(value), max(value), count(*) from ping_sensor where period(%s, 1, now(), timestamp) group by device_name order by min(timestamp);" % period 
            cmd = 'sql anylog format=json and stat=False "%s"' % query
            r, error = rest_query(self.config['query_conn'], command=cmd, query=True)
            if error != None: 
                print('Failed to GET row count (Error: %s)' % error)
                assert False 

            assert r.status_code == 200 
            try: 
                with open(self.config['actual_dir'] + '/%s' % results_file, 'w') as f: 
                    for row in r.json()['Query']: 
                        f.write(json.dumps(row) + '\n')
            except: 
                assert False
            else: 
                assert filecmp.cmp(self.config['expect_dir'] + '/%s' % results_file, self.config['actual_dir'] + '/%s' % results_file) == True
    ''' 
