import ast 
import configparser 
import json
import os
import sys

slash_char = '/'
if sys.platform.startswith('win'):
    slash_char = '\\'


def read_config(config_file:str)->dict: 
    """
    Read configuration file
    :args: 
        config_file:str - configuration file (full path)
    params: 
        data:dict - data from configuration 
        config:configparser.ConfigParser - call to configparser 
    :return: 
        data 
    """
    data = {} 
    try: 
        configs = configparser.ConfigParser()
    except Exception as e: 
        assert True == False, 'Failed to declare Parser (Error: %s)' % e
    if os.path.isfile(config_file):
        try:
            configs.read(config_file)
        except Exception as e: 
            assert True is False, 'Failed to read config_file %s (Error: %s)' % (config_file, e)
    else: 
        assert True is False, 'Config file: %s does not exist' % config_file
    try: 
        for section in configs.sections():
            for key in configs[section]:
                print(key)
                re_add = False
                if key == 'nodes' or key == 'anylog_api_info':
                    try:
                        data[key] = json.loads(configs[section][key])
                    except:
                        re_add = True
                if key == 'add_data' and configs[section][key].lower() == 'false':
                    data[key] = False
                elif key == 'add_data' and configs[section][key].lower() == 'true':
                    data[key] = True
                if key == 'anylog_api':
                    data[key] = os.path.expandvars(os.path.expanduser(configs[section][key]))
                if re_add is True:
                    try:
                        data[key] = ast.literal_eval(configs[section][key])
                    except:
                        data[key] = configs[section][key]
    except Exception as e: 
        assert True is False, 'Failed to extract variables from config file - %s (Error: %s)' % (config_file, e)

    return data


def write_file(query:str, data:list, results_file:str)->bool: 
    """
    Write to results file
    :args: 
        query:str - query executed
        data:list - list data 
        results_file:str - results file 
    :params:
        status:bool 
    :return: 
        status
    """
    try:
        with open(results_file, 'w') as f: 
            for row in data: 
                try: 
                    f.write(json.dumps(row) + '\n')
                except Exception as e: 
                    assert True == False, 'Failed to write line to file - %s(Error: %s).\n\tQuery: %s\n' % (results_file, e, query)
    except Exception as e: 
        assert True == False, 'Failed to open file: %s.\n\tQuery: %s (Error: %s)' % (results_file, e, query)


