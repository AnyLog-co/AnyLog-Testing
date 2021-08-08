import os 
import yaml 


def __format_config(config_file:str)->str: 
   config = os.path.expanduser(os.path.expandvars(config_file))
   
   if os.path.isfile(config): 
      return config
   else:
      print('Config file %s does not exist' % config_file) 
      return '' 

def read_config(config_file:str): 
   config_file = __format_config(config_file) 
   config_data = {} 
   try:
      with open(config_file) as f:
         config_data = yaml.load(f) 
   except Exception as e:
      print('Failed to retrive data from %s (Error: %s)' % (config_file, e))
      config_data = {} 
    
   if config_data != {}: 
      tmp = config_data['TABLE COLUMNS']
      config_data['TABLE COLUMNS'] = {} 
      for col in tmp: 
         config_data['TABLE COLUMNS'][list(col.keys())[0]] = list(col.values())[0]

   return config_data 


