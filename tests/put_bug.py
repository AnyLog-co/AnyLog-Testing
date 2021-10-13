import os
import requests

DATA_FILE = os.path.expanduser(os.path.expandvars('$HOME/AnyLog-Testing/data/al_smoke_test.ping_sensor.0.0.json'))
CONN = '10.0.0.92:2049'


header = {
    'type': 'json',
    'dbms': 'al_smoke_test',
    'table': 'ping_sensor',
    'mode': 'streaming',
    'Content-Type': 'text/plain',
    'User-Agent': 'AnyLog/1.23'
}


r = requests.get('http://%s' % CONN, headers={'command': 'get status', 'User-Agent': 'AnyLog/1.23'})
print(r.text)


with open(DATA_FILE, 'r') as f:
    for line in f.read().split('\n'):
        r = requests.put('http://%s' % CONN, headers=header, data=line)
        if int(r.status_code) != 200:
            print(r.status_code)
