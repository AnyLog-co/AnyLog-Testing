import requests

r = requests.get('http://10.0.0.78:7849', headers={'command': 'sql aiops "select value from ping_sensor limit 1"',
                                                    'User-Agent': 'AnyLog/1.23', 'destination': 'network'})
if int(r.status_code) != 200:
    print('Failed due to network error: %s' % r.status_code)
    exit(1)

value = r.json()['Query'][0]['value']
if isinstance(value, float):
    print(True)
else:
    print('%s instance type is: %s' % (value, type(value)))