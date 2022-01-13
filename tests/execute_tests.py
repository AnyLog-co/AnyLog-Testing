import filecmp
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
            pytest.fail(f"Failed to extract results from '{query}' (Error Code: {response['err_code']} | Error: {response['err_text']})")
        else:
            pytest.fail(f"Failed to extract results from '{query}' (Error: {e})")
    else:
        pytest.fail(f'Unable to validate response: {response}')

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
            del response[policy_type]['ledger']
            responses[index] = response
        assert file_io.write_file(query=query, file_name=actual_file, results=responses) is True
    elif isinstance(responses, str) or isinstance(responses, int):
        try:
            with open(actual_file, 'w') as f:
                try:
                    f.write(f'{query}\n{responses}')
                except Exception as e:
                    pytest.fail(f'Failed to write content to file {actual_file} (Error: {e})')
        except Exception as e:
            pytest.fail(f'Failed to open file: {actual_file} (Error: {e})')
    else:
        pytest.fail('Failed to validate content from query: %s' % query)
        if isinstance(responses, dict) and 'err_code' in responses and 'err_text' in responses:
            pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query,
                                                                                              responses['err_code'],
                                                                                              responses['err_text']))
    assert filecmp.cmp(actual_file, expected_file) is True

