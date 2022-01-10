def test_increments_1minute(self):
    """
    Test increments by minute
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(minute, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY MIN(timestamp) DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1minute.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1minute.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_30minute(self):
    """
    Test increments by 30 minutes
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(minute, 30, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp <= NOW() + 1 month ORDER BY MIN(timestamp) ASC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_30minute.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_30minute.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_1hour(self):
    """
    Test increments by 1 hour
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(hour, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp >= '2021-12-01 00:00:00' AND "
             + "timestamp <= '2021-12-31 23:59:59' ORDER BY max(timestamp) DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1hour.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1hour.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_12hour(self):
    """
    Test increments by 12 hour
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(hour, 12, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), MAX(value), "
             + "AVG(value) FROM ping_sensor WHERE timestamp <= '2022-12-15 00:00:00' OR "
               "timestamp >= '2022-01-15 23:59:59' ORDER BY max(timestamp) ASC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_12hour.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_12hour.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_day(self):
    """
    Test increments by day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(day, 1, timestamp), parentelement, MIN(timestamp), MAX(timestamp), MIN(value), "
             + "MAX(value), AVG(value) FROM ping_sensor GROUP BY parentelement "
             + "ORDER BY parentelement, MIN(timestamp) DESC")

    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_day.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_day.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_5day(self):
    """
    Test increments by 5 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(day, 5, timestamp), device_name, MIN(timestamp), MAX(timestamp), MIN(value), "
             + "MAX(value), AVG(value) FROM ping_sensor GROUP BY device_name "
             + "ORDER BY device_name, MIN(timestamp) ASC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_5day.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_5day.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_7day(self):
    """
    Test increments by 7 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
             + "MAX(value), AVG(value) FROM ping_sensor WHERE device_name='Catalyst 3500XL' "
             + "ORDER BY MAX(timestamp) ASC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_7day.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_7day.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_15day(self):
    """
    Test increments by 15 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(day, 7, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
             + "MAX(value), AVG(value) FROM ping_sensor WHERE parentelement='62e71893-92e0-11e9-b465-d4856454f4ba' "
             + "ORDER BY MAX(timestamp) DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_15day.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_15day.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_increments_1month(self):
    """
    Test increments by month
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:list - results from query
        excepted_file:str - file containing expected results
        actual_file:str - file updated with results
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
    """
    query = ("SELECT increments(month, 21, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), "
             + "MAX(value), AVG(value) FROM ping_sensor ORDER BY MAX(timestamp) DESC")

    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    excepted_file = os.path.join(EXPECTED_DIR, 'test_increments_1month.json')
    actual_file = os.path.join(ACTUAL_DIR, 'test_increments_1month.json')

    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (
                        query, response['err_code'], response['err_text']))
                else:
                    pytest.fail(
                        "Failed to extract results from '%s' (Error: %s)" % (query, e))
            else:
                assert file_io.write_file(file_name=actual_file, results=results) is True
                assert filecmp.cmp(actual_file, excepted_file)
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_1minute(self):
    """
    Test period by minute
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
    :query:
        SELECT
            timestamp, value
        FROM
            ping_sensor
        WHERE
            period(minute, 1, '2022-02-05 18:27:43.748009', timestamp)
        ORDER BY
            timestamp DESC
    :assert:
        unique UUID vales
    """
    query = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 1, '2022-02-05 18:27:43.748009', timestamp) ORDER BY timestamp DESC"
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query'][0]
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == {'timestamp': '2022-01-29 11:51:14.136243', 'value': 4.17}
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_30minute(self):
    """
    Test period by 30 minute
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = "SELECT timestamp, value FROM ping_sensor WHERE period(minute, 30, '2022-02-05 18:27:43.748009', timestamp) ORDER BY timestamp ASC"
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query'][0]
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == {'timestamp': '2022-01-29 11:51:14.136243', 'value': 4.17}
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_1hour(self):
    """
    Test period by 1 hour
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
    :query:
        SELECT
            timestamp, value
        FROM
            ping_sensor
        WHERE
            period(hour, 1, '2022-01-01 00:00:00', timestamp)
    :assert:
        unique UUID vales
    """
    query = "SELECT timestamp, value FROM ping_sensor WHERE period(hour, 1, '2022-01-01 00:00:00', timestamp)"
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query'][0]
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == {'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16}
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_12hour(self):
    """
    Test period by 12 hour
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = "SELECT timestamp, value FROM ping_sensor WHERE period(hour, 12, '2022-01-01 00:00:00', timestamp) ORDER BY timestamp DESC"
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                   {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29}]
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_day(self):
    """
    Test period by day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = "SELECT timestamp, value FROM ping_sensor WHERE period(day, 1, '2021-12-31 23:59:59', timestamp) ORDER BY timestamp DESC"
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == [{'timestamp': '2021-12-31 06:57:33.344011', 'value': 2.16},
                                   {'timestamp': '2021-12-31 02:46:59.258990', 'value': 0.29},
                                   {'timestamp': '2021-12-30 08:07:28.173834', 'value': 2.16}]
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_5day(self):
    """
    Test period by 5 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = ("SELECT device_name, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value) "
             + "FROM ping_sensor WHERE period(day, 5, '2022-01-31 00:00:00', timestamp) GROUP BY device_name "
             + "ORDER BY device_name ASC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
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
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_7day(self):
    """
    Test period by 7 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = ("SELECT parentelement, min(timestamp), max(timestamp), min(value), max(value), avg(value), count(value) "
             + "FROM ping_sensor WHERE period(day, 7, '2022-01-31 00:00:00', timestamp) GROUP BY parentelement "
             + "ORDER BY parentelement DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
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
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_15day(self):
    """
    Test period by 15 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = ("SELECT timestamp, value "
             + "FROM ping_sensor WHERE period(day, 15, '2022-01-31 00:00:00', timestamp) and "
             + "device_name = 'ADVA FSP3000R7' ORDER BY timestamp DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == [{'timestamp': '2022-01-28 09:08:20.155750', 'value': 0.5},
                                   {'timestamp': '2022-01-27 16:17:02.263903', 'value': 1.84},
                                   {'timestamp': '2022-01-25 14:18:42.213060', 'value': 0.29},
                                   {'timestamp': '2022-01-25 09:15:16.139743', 'value': 3.64},
                                   {'timestamp': '2022-01-18 20:59:36.193183', 'value': 0.69}]
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')


def test_period_1month(self):
    """
    Test period by 15 day
    :params:
        query:str - SQL statement to execute
        headers:dict - REST headers
        results:dict - results from query
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
        unique UUID vales
    """
    query = ("SELECT timestamp, value "
             + "FROM ping_sensor WHERE period(day, 15, '2022-01-31 00:00:00', timestamp) and "
             + "device_name = 'ADVA FSP3000R7' ORDER BY timestamp DESC")
    headers = {
        'command': 'sql %s format=json and stat=false "%s"' % (self.configs['dbms'], query),
        'User-Agent': 'AnyLog/1.23',
        'destination': 'network'
    }
    if self.status is True:
        response = self.anylog_conn.get(headers=headers)
        if isinstance(response, dict):
            try:
                results = response['Query']
            except Exception as e:
                if 'err_code' in response and 'err_text' in response:
                    pytest.fail("Failed to extract results from 'DISTINCT(value)' (Error Code: %s | Error: %s)" % (
                        response['err_code'], response['err_text']))
                else:
                    pytest.fail("Failed to extract results from 'DISTINCT(parentelement)' (Error: %s)" % e)
            else:
                assert results == [{'timestamp': '2022-01-28 09:08:20.155750', 'value': 0.5},
                                   {'timestamp': '2022-01-27 16:17:02.263903', 'value': 1.84},
                                   {'timestamp': '2022-01-25 14:18:42.213060', 'value': 0.29},
                                   {'timestamp': '2022-01-25 09:15:16.139743', 'value': 3.64},
                                   {'timestamp': '2022-01-18 20:59:36.193183', 'value': 0.69}]
        else:
            pytest.fail(response)
    else:
        pytest.fail('Failed to validate connection to AnyLog')