import os
import pytest
import sys

ROOT_DIR = os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')
sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest

def execute_sql(anylog_conn:rest.RestCode, headers:dict, query:str, expected_file:str, actual_file:str):
    """
    Execute and validate SQL query
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        headers:dict - REST headers
        query:str - query that was executed
        expected_file:str - file containing expected results
        actual_file:str - file containing results from latest run
    :params:
        response:dict - raw results from
        results:list - list of results from response
    :assert:
        1. content written to file
        2. compare actual vs expected results
    """
    response = anylog_conn.get(headers=headers)
    if isinstance(response, dict):
        if 'Query' in response:
            results = response['Query']
            assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
        elif 'err_code' in response and 'err_text' in response:
            pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query,
                                                                                              response['err_code'],
                                                                                              response['err_text']))
        else:
            pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
    else:
        pytest.fail('Unable to validate response: %s' % response)

    assert filecmp.cmp(actual_file, expected_file)


def execute_blockchain(anylog_conn:rest.RestCode, headers:dict, query:str, expected_file:str, actual_file:str):
    """
    Execute and validate blockchain query
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        headers:dict - REST headers
        query:str - query that was executed
        expected_file:str - file containing expected results
        actual_file:str - file containing results from latest run
    :params:
        response:list - list of blockchain from query
        results:list - list of results from response
    :assert:
        1. content written to file
        2. compare actual vs expected results
    """
    responses = anylog_conn.get(headers=headers)
    if isinstance(responses, list):
        # remove 'date' from policy(s) to create consistent results
        for index in range(len(responses)):
            response = responses[index]
            policy_type = list(response)[0]
            del response[policy_type]['date']
            responses[index] = response
        assert file_io.write_file(query=query, file_name=expected_file, results=responses) is True
    elif isinstance(responses, str) or isinstance(responses, int):
        try:
            with open(expected_file, 'w') as f:
                try:
                    f.write('%s\n%s' % (query, responses))
                except Exception as e:
                    pytest.fail('Failed to write content to file %s (Error: %s)' % (expected_file, e))
        except Exception as e:
            pytest.fail('Failed to open file: %s' % (expected_file, e))
    else:
        pytest.fail('Failed to validate content from query: %s' % query)
        if isinstance(responses, dict) and 'err_code' in responses and 'err_text' in responses:
            pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query,
                                                                                              responses['err_code'],
                                                                                              responses['err_text']))

