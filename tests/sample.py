import requests

for param in ['min(value)', 'max(value)', 'avg(value)', 'sum(value)', 'count(distinct(value))']:
    r = requests.get('http://10.0.0.78:7849', headers={'command': 'sql lsl_demo format=json and stat=false "select %s from ping_sensor"' % param,
                                                        'User-Agent': 'AnyLog/1.23', 'destination': 'network'})
    if int(r.status_code) != 200:
        print('Failed due to network error: %s' % r.status_code)
        exit(1)

    value = r.json()['Query'][0][param]
    print('Query: %s | Data Type: %s' % (param, type(value)))


"""
Results:
Query: min(value) | Data Type: <class 'str'>
Query: max(value) | Data Type: <class 'str'>
Query: avg(value) | Data Type: <class 'float'>
Query: sum(value) | Data Type: <class 'str'>
Query: count(distinct(value)) | Data Type: <class 'int'> 
"""