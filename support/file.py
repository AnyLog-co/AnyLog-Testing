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
        print('Failed to declare Parser (Error: %s)' % e)
    if os.path.isfile(config_file):
        try:
            config.read(config_file)
        except Exception as e: 
            print('Failed to read config_file %s (Error: %s)' % (config_file, e))
    else: 
        print('Config file: %s does not exist' % config_file) 
    try: 
        for section in config.sections():
            for key in config[section]:
                try: 
                    data[key] = ast.literal_eval(config[section][key])
                except: 
                    data[key] = config[section][key]
    except Exception as e: 
        print('Failed to extract variables from config file (Error: %s)' % e)

    return data 


def write_file(data:list, results_file:str)->bool: 
    """
    Write to results file
    :args: 
        data:list - list data 
        results_file:str - results file 
    :params:
        status:bool 
    :return: 
        status
    """
    status = True 
    try:
        with open(results_file, 'w') as f: 
            for row in data: 
                try: 
                    f.write(json.dumps(row) + '\n')
                except Exception as e: 
                    print('Failed to write line to file (Error: %s)' % e)
                    status = False 
    except Exception as e: 
        print('Failed to open file: %s (Error: %s)' % (results_file, e))
        status = False

    return status

