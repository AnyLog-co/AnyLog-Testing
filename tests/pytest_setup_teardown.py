import filecmp
import os
import pytest
import shutil
import sys

ROOT_DIR = os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')
sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest
import send_data


def setup_code(config_file:str, expected_dir:str, actual_dir:str) -> (bool, dict, rest.RestCode):
    """
    Setup code for pytest(s)
    :args:
        table_name:list - list of tables to store data for
        config_file:str - configurations file
        data_dir:str - directory containing data sets used for testing
        expected_dir:str - directory containing expected result dir
        actual_dir:str - directory containing actual results
    :params:
        status:bool - whether or not able to access node
        configs:str - configurations from config_file
        anylog_rest:rest.RestCode - class connected easily execute REST against AnyLog
        payloads:list - data extracted to be stored in AnyLog
    :return:
        status, configs
    """
    status = False
    # validate directories
    if not os.path.isdir(expected_dir):
        pytest.fail('Failed to locate expected results dir')
    if not os.path.isdir(actual_dir):
        try:
            os.makedirs(actual_dir)
        except Expection as e:
            pytes.fail('Failed to created directory: %s (Error: %s)' % (actual_dir, e))

    # extract configs
    configs = file_io.read_configs(config_file=config_file)

    # connect to AnyLog & validate connection
    anylog_conn = rest.RestCode(conn=configs['conn'], user=configs['rest_user'], password=configs['rest_password'],
                                timeout=configs['timeout'])

    response = anylog_conn.get(headers={'command': 'get status', 'User-Agent': 'AnyLog/1.23'})
    assert 'running' in response and 'not' not in response
    if 'running' in response and 'not' not in response:
        status = True

    return status, configs, anylog_conn

def write_data(data_dir:str, send_type:str, dbms:str, tables:list, anylog_conn:rest.RestCode=None, topic:str=None,
               broker:str=None, port:int=None, username:str=None, password:str=None):
    """
    Write content into AnyLog
    :args:
        table_name:list - list of tables to store data for
        configs:str - configurations from config_file
        data_dir:str - directory containing data sets used for testing
    :params:
        status:bool
        payloads:list - data extracted to be stored in AnyLog
    :return:
        status
    """
    for fn in os.listdir(data_dir):
        for table in tables:
            if table in fn:
                file_name = os.path.join(data_dir, fn)
                payloads = file_io.json_read_file(file_name=file_name)
                send_data.store_payloads(send_type=send_type, payloads=payloads, dbms=dbms, table=table,
                                         anylog_conn=anylog_conn,topic=topic, broker=broker, port=port, username=username,
                                         password=password)


def execute_test(anylog_conn:rest.RestCode, headers:dict, query:str, expected_file:str, actual_file:str):
    """
    Query to execute & validate
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        headers:dict - REST headers
        query:str - query that was executed
        expected_file:str - file containing expected results
        actual_file:str - file containing results from latest run
    :params:
        response:dict - raw results from
        results:list - list of results from response
    """
    response = anylog_conn.get(headers=headers)
    if isinstance(response, dict):
        if 'Query' in response:
            results = response['Query']
        elif 'err_code' in response and 'err_text' in response:
            pytest.fail("Failed to extract results from '%s' (Error Code: %s | Error: %s)" % (query,
                                                                                              response['err_code'],
                                                                                              response['err_text']))
        else:
            pytest.fail("Failed to extract results from '%s' (Error: %s)" % (query, e))
        assert file_io.write_file(query=query, file_name=actual_file, results=results) is True
        assert filecmp.cmp(actual_file, expected_file)
    else:
        pytest.fail(response)


def teardown_code(actual_dir:str):
    if os.path.isdir(actual_dir):
        try:
            shutil.rmtree(actual_dir)
        except Exception as e:
            pytest.fail('Failed to drop actual results directory: %s (Error: %s)' % (actual_dir, e))

