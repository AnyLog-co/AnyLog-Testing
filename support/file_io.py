import configparser
import os
import pytest
import support
import time

last_timestamp = 0

def generate_file_name(results_dir:str)->str:
    """
    Generate a timestamp based file name
    :args:
        results_dir:str - directory that'll contain results
    :params:
        global last_timestamp:int - last timestamp
        current_timestamp:int - timestamp as integer
        full_path:str - full file path
    :return:
        full_path
    """
    global last_timestamp

    current_time = int(time.time())
    if current_time <= last_timestamp:
        last_timestamp += 1
    else:
        last_timestamp = current_time

    return os.path.join(results_dir, 'output_%s.rslts' % str(last_timestamp))

def read_file(file_name:str)->list:
    """
    Given a (JSON) file, read its contents and writen them as a list
    :args:
        file_name:str - file to reead
        dbms:str - logical database name
    :params:
        payloads:dict - content from file
    :return:
        payloads
    """
    payloads = []
    table_name = file_name.split('.')[1]

    if os.path.isfile(file_name):
        try:
            with open(file_name, 'r') as f:
                try:
                    for line in f.read().split("\n"):
                        if line != '':
                            line = support.json_loads(line)
                            payloads.append(line)
                except Exception as e:
                    pytest.fail('Failed to read line(s) (Error: %s)' % e)
        except Exception as e:
            pytest.fail("Failed to open file '%s' (Error: %s)" % (file_name, e))

    return payloads



def write_file(query:str, file_name:str, results:list)->bool:
    """
    Write results to file
    :args:
        query:str - query being executed
        file_name:str - file to write content into
        results:list - content to write into files
    :params:
        status:bool
    :return:
        status
    """
    status = True
    try:
        with open(file_name, 'w') as f:
            try:
                f.write(query)
            except Exception as e:
                pytest.fail('Failed to write query to file (Error: %s)' % e)
                status = False
    except Exception as e:
        pytest.fail('Failed to open file %s (Error: %s)' % (file_name, e))
        status = False        
    try:
        with open(file_name, 'a') as f:
            for result in results:
                try:
                    f.write('\n' + support.json_dumps(result))
                except Exception as e:
                    pytest.fail('Failed to write to file: %s (Error: %s)' % (file_name, e))
                    status = False
    except Exception as e:
        pytest.fail('Failed to open file %s (Error: %s)' % (file_name, e))
        status = False

    return status


def read_configs(config_file:str)->dict:
    """
    Read INI configuration & store in dict 
    :args: 
        config_file:str - configuraiton file
    :params: 
        data:dict - data from config file that files to to be added to AnyLog Network
        error_msgs:list - if something fails, list of error messages
        config_full_path:str - full path of configuration file 
    :return: 
        data 
    """
    data = {}
    status = True
    config = configparser.ConfigParser()
    if os.path.isfile(config_file):
        try:
            config.read(config_file)
        except Exception as e: 
            pytest.fail('Failed to read config file: %s (Error: %s)' % (config_file, e))
            status = False
    else:
        pytest.fail('File %s not found' % config_file)
        status = False

    if status is True:
        try:
            for section in config.sections():
                for key in config[section]:
                    data[key] = config[section][key].replace('"', '')
                    if key == 'timeout':
                        data[key] = int(data[key])
        except Exception as e:
            pytest.fail('Failed to extract variables from config file (Error: %s)' % e)

    return data
