import configparser
import os
import pytest
import support

def read_file(file_name:str)->list:
    """
    Given a (JSON) file, read its contents and writen them as a list
    :args:
        file_name:str - file to reead
    :params:
        payloads:dict - content from file
    :return:
        payloads
    """
    payloads = []
    if os.path.isfile(file_name):
        try:
            with open(file_name, 'r') as f:
                try:
                    for line in f.read().split("\n"):
                        if line != '':
                            payloads.append(support.json_loads(line))
                except Exception as e:
                    pytest.fail('Failed to read line(s) (Error: %s)' % e)
        except Exception as e:
            pytest.fail("Failed to open file '%s' (Error: %s)" % (file_name, e))
    return payloads


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
        except Exception as e:
            pytest.fail('Failed to extract variables from config file (Error: %s)' % e)

    return data
