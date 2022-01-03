import os
import pytest
import shutil
import sys

ROOT_DIR = os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')
sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest_get
import send_data


def setup_code(table_name:list, config_file:str, data_dir:str, expected_dir:str, actual_dir:str)->(bool, dict):
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
        payloads:list - data extracted to be stored in AnyLog
    :return:
        status, configs
    """
    if not os.path.isdir(expected_dir):
        pytest.fail('Failed to locate expected results dir')
    if not os.path.isdir(actual_dir):
        try:
            os.makedirs(actual_dir)
        except Expection as e:
            pytes.fail('Failed to created directory: %s (Error: %s)' % (actual_dir, e))

    configs = file_io.read_configs(config_file=config_file)
    status = rest_get.get_status(conn=configs['conn'], username=configs['rest_user'],
                                      password=configs['rest_password'])
    if status is True:
        for fn in os.listdir(data_dir):
            for table in table_name:
                if table in fn:
                    file_name = os.path.join(data_dir, fn)
                    payloads = file_io.read_file(file_name=file_name, dbms=configs['dbms'])
        if configs['insert'] == 'true':
            send_data.store_payloads(payloads=payloads, configs=configs)

    return status, configs


def teardown_code(actual_dir:str):
    if os.path.isdir(actual_dir):
        try:
            shutil.rmtree(actual_dir)
        except Exception as e:
            pytest.fail('Failed to drop actual results directory: %s (Error: %s)' % (actual_dir, e))

