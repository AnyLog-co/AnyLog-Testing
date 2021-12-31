from paho.mqtt import client
import pytest
import random
import requests
import support
import time

def post_data(conn:str, data:list, dbms:str, table:str=None, rest_topic:str='new-topic', auth:tuple=None,
              timeout:int=30, exception:bool=False)->bool:
    """
    Send data via REST using POST command
    :notes:
        URL: https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-post-command
        Comment: requires MQTT client call on the accepting AnyLog side
    :args:
        conn:str - REST connection information
        data - either a list or dict of data sets
        rest_topic:str - MQTT topic
        dbms:str - logical database name
        table:str - table name, if data is dict use keys as table name(s)
        auth:tuple - Authentication username + password
        timeout:nt - wait time
        exception:bool - whether or not to pytest.fail error messages
    :params:
        status:bool
        headers:dict - REST header info
        payloads:list - content to POST
    :return:
        status
    """
    status = True
    headers = {
        'command': 'data',
        'topic': rest_topic,
        'User-Agent': 'AnyLog/1.23',
        'Content-Type': 'text/plain'
    }

    payloads = support.payload_conversions(payloads=data, dbms=dbms, table=table)
    for row in payloads:
        try:
            r = requests.post(url='http://%s' % conn, headers=headers, data=payload, auth=auth, timeout=timeout)
        except Exception as e:
            if exception is True:
                pytest.fail('Failed to POST content into %s (Error: %s)' % (conn, e))
            status = False
        else:
            if int(r.status_code) != 200 and exception is True:
                pytest.fail('Failed to POST content into %s (Network Error: %s)' % (conn, r.status_code))
                status = Fasle
            elif int(r.status_code) != 200:
                status = False

    return status


def put_data(conn:str, data:list, auth:tuple=None, timeout:int=30, exception:bool=False)->bool:
    """
    Send data via REST using PUT command
    :url:
        https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-put-command
    :args:
        conn:str - REST connection information
        data - either a list or dict of data sets
        dbms:str - logical database name
        table:str - table name, if data is dict use keys as table name(s)
        auth:tuple - Authentication username + password
        timeout:nt - wait time
        exception:bool - whether or not to pytest.fail error messages
    :params:
        status:bool
        headers:dict - REST header info
        payloads:list - content to POST
    :return:
        status
    """
    status = True
    headers = {
        'type': 'json',
        'dbms': None,
        'table': None,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }
    if isinstance(data, list):
        for row in data:
            headers['dbms'] = row['dbms']
            headers['table'] = row['table']
            del row['dbms']
            del row['table']
            try:
                r = requests.put(url='http://%s' % conn, headers=headers, data=support.json_dumps(row), auth=auth, timeout=timeout)
            except Exception as e:
                if exception is True:
                    pytest.fail('Failed to PUT data into %s (Error: %s)' % (conn, e))
                status = False
            else:
                if int(r.status_code) != 200 and exception is True:
                    pytest.fail('Failed to PUST data into %s (Network Error: %s)' % r.status_code)
                    status = False
                elif int(r.status_code) != 200:
                    status = False

    elif isinstance(data, dict):
        for table in data:
            headers['table'] = table
            for row in data[table]:
                try:
                    r = requests.put(url='http://%s' % conn, headers=headers, data=support.json_dumps(row), auth=auth, timeout=timeout)
                except Exception as e:
                    if exception is True:
                        pytest.fail('Failed to PUT data into %s (Error: %s)' % (conn, e))
                    status = False
                else:
                    if int(r.status_code) != 200 and exception is True:
                        pytest.fail('Failed to PUST data into %s (Network Error: %s)' % r.status_code)
                        status = False
                    elif int(r.status_code) != 200:
                        status = False
    return status


def connect_mqtt_broker(broker:str, port:int, username:str=None, password:str=None, exception:bool=True):
    """
    Connect to an MQTT broker
    :args:
        broker:str - MQTT broker IP
        port:int - MQTT broker port
        username:str - MQTT broker user
        password:str - MQTT broker password correlated to user
    :params:
        mqtt_client_id:str - MQTT client ID
        client:paho.mqtt.client.Client - MQTT client object
    :return:
        client
    """
    # connect to MQTT broker
    status = True
    mqtt_client_id = 'python-mqtt-%s' % random.randint(random.choice(range(0, 500)), random.choice(range(501, 1000)))

    try:
        mqtt_client = client.Client(mqtt_client_id)
    except Exception as e:
        if exception is True:
            pytest.fail('Failed to set MQTT client ID (Error: %s)' % e)
        mqtt_client = None

    # set username and password
    if mqtt_client is not None and username is not False and password is not None:
        try:
            mqtt_client.username_pw_set(username, password)
        except Exception as e:
            if exception is True:
                pytest.fail('Failed to set MQTT username & password (Error: %s)' % e)
            mqtt_client = None

    # connect to broker
    if mqtt_client is not None:
        try:
            mqtt_client.connect(broker, int(port))
        except Exception as e:
            if exception is True:
                pytest.fail('failed to connect to MQTT broker %s against port %s (Error: %s)' % (broker, port, e))
            mqtt_client = None

    return mqtt_client


def mqtt_send_data(mqtt_client:client.Client, topic:str, data:dict, dbms:str, table:str, exception:bool=False)->bool:
    """
    Send data into an MQTT broker
    :args:
        mqtt_client:paho.mqtt.client.Client - MQTT broker client
        topic:str - topic to send data into
        data:dict - either list or dict of data to send into MQTT broker
        dbms:str - logical database
        table:str - logical table name
        exception:bool - whether or not to pytest.fail exceptions
    :params:
        status:bool
        payloads:list - converted data
        r:paho.mqtt.client.MQTTMessageInfo - result from publish process
    :return:
        status
    """
    status = True
    payloads = support.payload_conversions(payloads=data, dbms=dbms, table=table)
    for message in payloads:
        try:
            r = mqtt_client.publish(topic, message, qos=1, retain=False)
        except Exception as e:
            if exception is True:
                pytest.fail('Failed to publish results in %s (Error: %s)' % (mqtt_client.conn, e))
            status = False
        else:
            time.sleep(5)
            if r[0] != 0 and exception is True:
                pytest.fail('There was a network error when publishing content')
                status = False
            elif r[0] != 0:
                status = False

    return status


def store_payloads(payloads:list, configs:dict)->bool:
    """
    Send payloads into either AnyLog or MQTT broker based on configs
    :args:
        payloads:list - content to store
        configs:dict - configurations
    :params:
        status:bool
        auth:tuple - REST authentication params
    :return:
        status
    """
    status = True
    auth = (configs['rest_user'], configs['rest_password'])
    if configs['rest_user'] == '' or configs['rest_password']:
        auth = None

    if configs['send'] == 'put':
        status = put_data(conn=configs['conn'], data=payloads, auth=auth, timeout=30, exception=True)
    elif configs['send'] == 'post':
        status = post_data(conn=configs['conn'], data=payloads, dbms=configs['dbms'], table=configs['table'],
                           rest_topic=configs['topic'], auth=auth, timeout=30, exception=True)
    elif configs['send'] == 'mqtt':
        mqtt_conn = connect_mqtt_broker(broker=configs['broker'], port=configs['port'],
                                             username=configs['mqtt_user'], password=configs['password'])
        if mqtt_conn is not None:
            status = mqtt_send_data(mqtt_client=mqtt_conn, topic=configs['topic'], data=payloads, dbms=configs['dbms'],
                                    table=configs['table'], exception=True)
    time.sleep(70)
    return status