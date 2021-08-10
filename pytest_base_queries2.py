

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
