import ast 
import configparser 
import json
import os 

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
        config = configparser.ConfigParser()
    except Exception as e: 
        assert True == False, 'Failed to declare Parser (Error: %s)' % e
    if os.path.isfile(config_file):
        try:
            config.read(config_file)
        except Exception as e: 
            assert True == False, 'Failed to read config_file %s (Error: %s)' % (config_file, e)
    else: 
        assert True == False, 'Config file: %s does not exist' % config_file 
    try: 
        for section in config.sections():
            for key in config[section]:
                try: 
                    data[key] = ast.literal_eval(config[section][key])
                except: 
                    data[key] = config[section][key]
    except Exception as e: 
        assert True == False, 'Failed to extract variables from config file - %s (Error: %s)' % (config_file, e)

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


