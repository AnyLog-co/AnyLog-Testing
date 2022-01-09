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


def setup_code(config_file:str, expected_dir:str, actual_dir:str)->(bool, dict, rest.RestCode):
    """
    Setup code for pytest(s)
    :process:
        1. validate expected results directory
        2. create actual results directory
        3. extract data used for testing
        4. validate node provided is connected
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
    if not os.path.isdir(expected_dir):
        pytest.fail('Failed to locate expected results dir')
    if not os.path.isdir(actual_dir):
        try:
            os.makedirs(actual_dir)
        except Expection as e:
            pytes.fail('Failed to created directory: %s (Error: %s)' % (actual_dir, e))

    configs = file_io.read_configs(config_file=config_file)

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
                payloads = file_io.read_file(file_name=file_name)
                send_data.store_payloads(send_type=send_type, payloads=payloads, dbms=dbms, table=table,
                                         anylog_conn=anylog_conn,topic=topic, broker=broker, port=port, username=username,
                                         password=password)

def teardown_code(actual_dir:str):
    if os.path.isdir(actual_dir):
        try:
            shutil.rmtree(actual_dir)
        except Exception as e:
            pytest.fail('Failed to drop actual results directory: %s (Error: %s)' % (actual_dir, e))

