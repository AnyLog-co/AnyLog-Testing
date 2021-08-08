import ast 
import configparser 
import os 

def read_config(config_file)->dict: 
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


