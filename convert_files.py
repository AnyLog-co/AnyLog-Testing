import json
import os

DIR_PATH=os.path.expandvars(os.path.expanduser('$HOME/AnyLog-Testing/customers'))
NEW_FILE=os.path.expandvars(os.path.expanduser('$HOME/AnyLog-Testing/data/al_smoke_test.aiops.0.0.json'))
open(NEW_FILE, 'w').close()

tables = []
for file in os.listdir(DIR_PATH):
    if '.json' in file:
        with open(DIR_PATH + "/" + file, 'r') as f:
            for line in f.readlines():
                new_dict = json.loads(line)
                new_dict['dbms'] = 'al_smoke_test'
                if file.split('.')[1] not in tables:
                    tables.append(file.split('.')[1])
                new_dict['table'] = file.split('.')[1]
                with open(NEW_FILE, 'a') as f:
                    f.write(json.dumps(new_dict) + '\n')

print(tables, len(tables))