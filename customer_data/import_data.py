import os
import json

DATA_DIR=os.path.expandvars(os.path.expanduser('$HOME/AnyLog-Testing/customer_data'))
FILE_PATH=os.path.expandvars(os.path.expanduser('$HOME/AnyLog-Testing/data/al_smoke_test.aiops.0.0.json'))

open(FILE_PATH, 'w').close()

for fn in os.listdir(DATA_DIR):
    if 'aiops.fic' in fn:
        full_path = DATA_DIR + '/' + fn
        with open(full_path, 'r') as f:
            for line in f.readlines():
                data = json.loads(line)
                data['dbms'] = 'al_smoke_test'
                data['table'] = fn.split('.')[1]
                with open(FILE_PATH, 'a') as f:
                    f.write(json.dumps(data) + '\n')
