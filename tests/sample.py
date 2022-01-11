import datetime
import filecmp
import os
import pytest
import sys

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

CONFIG_FILE = os.path.join(ROOT_DIR, 'configs', 'sample_config.ini') # will be replaced by user param
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect', 'lit_san_leandro_single_table_queries')
ACTUAL_DIR = os.path.join(ROOT_DIR, 'actual')

sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest
import send_data
import support



def store_data(data_dir:str, data_set_file:str, send_type:str, dbms:str, anylog_conn:rest.RestCode=None, topic:str=None,
               broker:str=None, port:int=None, username:str=None, password:str=None)->bool:
    """
    Function to store data
    :args:
        data_dir:str - directory containing data
        data_set_file:str - file containing datasets to use
        send_type:str - send type [PUT, POST, MQTT]
        dbms:str - logical database name
        anylog_conn:rest.RestCode - rest connection
        topic:str - MQTT or POST topic
        broker:str - MQTT broker
        port:int - MQTT port
        username:str - MQTT user
        password:Str - MQTT password
    :params:
        data_sets:tuple - tuple of data sets to store (also used for table name(s)

    """
    data_sets = ()
    data_set_file = os.path.expanduser(os.path.expandvars(data_set_file))
    if os.path.isfile(data_set_file):
        try:
            with open(data_set_file, 'r') as f:
                try:
                    data_sets = f.readlines()
                except Exception as e:
                    pytest.fail('Failed to read content in file %s (Error: %s)' % (data_set_file, e))
        except Exception as e :
            pytest.fail('Failed to open file %s (Error: %s)' % (data_set_file ,e))
        else:
            for i in range(len(data_sets)):
                data_sets[i] = data_sets[i].split('\n')[0]
            data_sets = tuple(data_sets)
    else:
        pytest.fail('Failed to locate data_set file: "%s"' % data_set_file)

    for data_set in data_sets:
        for fn in os.listdir(data_dir):
            if data_set in fn:
                file_name = os.path.join(data_dir, fn)
                payloads = file_io.json_read_file(file_name=file_name)
                send_data.store_payloads(send_type=send_type, payloads=payloads, dbms=dbms, table=table,
                                         anylog_conn=anylog_conn, topic=topic, broker=broker, port=port,
                                         username=username, password=password)


if __name__ == '__main__':
    print(setup_code(config_file=CONFIG_FILE, expected_dir=EXPECTED_DIR, actual_dir=ACTUAL_DIR))