import os
from paho.mqtt import client
import pytest
import random
import requests
import support
import sys
import time

ROOT_DIR = os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')
sys.path.insert(0, SUPPORT_DIR)
import file_io
import rest
import support


def put_data(anylog_conn:rest.RestCode, payloads:list, dbms:str, table:str):
    """
    Send data via REST using PUT command
    :url:
        https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-put-command
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        payloads:list - list of dicts to store in AnyLog
        dbms:str - logical database to store data in
        table:str - table to store data in
    :params:
        headers:dict - REST header
    """
    headers = {
        'type': 'json',
        'dbms': dbms,
        'table': table,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }
    for payload in payloads:
        if isinstance(payload, dict):
            payload = support.json_dumps(payload)
        if isinstance(payload, str):
            anylog_conn.put(headers=headers, payload=payload)


def post_data(anylog_conn:rest.RestCode, topic:str, payloads:list, dbms:str, table:str):
    """
    Send data via REST using POST command
    :url:
        https://github.com/AnyLog-co/documentation/blob/master/adding%20data.md#using-a-post-command
    :comment:
        requires MQTT client call on the accepting AnyLog side
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        payloads:list - list of dicts to store in AnyLog
        dbms:str - logical database to store data in
        table:str - table to store data in
    :params:
        headers:dict - REST header
    """
    headers = {
        'command': 'data',
        'topic': topic,
        'User-Agent': 'AnyLog/1.23',
        'Content-Type': 'text/plain'
    }

    for payload in payloads:
        payload['dbms'] = dbms
        payload['table'] = table
        if isinstance(payload, dict):
            payload = support.json_dumps(payload)
        if isinstance(payload, str):
            anylog_conn.post(headers=headers, payload=payload)


def connect_mqtt_broker(broker:str, port:int, username:str=None, password:str=None)->client.Client:
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
        pytest.fail('Failed to set MQTT client ID (Error: %s)' % e)

    # set username and password
    if mqtt_client is not None and username is not False and password is not None:
        try:
            mqtt_client.username_pw_set(username, password)
        except Exception as e:
            pytest.fail('Failed to set MQTT username & password (Error: %s)' % e)

    # connect to broker
    try:
        mqtt_client.connect(broker, int(port))
    except Exception as e:
        pytest.fail('failed to connect to MQTT broker %s against port %s (Error: %s)' % (broker, port, e))

    return mqtt_client


def mqtt_send_data(mqtt_client:client.Client, topic:str, payloads:dict, dbms:str, table:str)->bool:
    """
    Send data into an MQTT broker
    :args:
        mqtt_client:paho.mqtt.client.Client - MQTT broker client
        topic:str - topic to send data into
        payloads:dict - list of data to send into MQTT broker
        dbms:str - logical database
        table:str - logical table name
        exception:bool - whether or not to pytest.fail exceptions
    :params:
        status:bool
        response:paho.mqtt.client.MQTTMessageInfo - result from publish process
    """
    for payload in payloads:
        payload['dbms'] = dbms
        payload['table'] = table
        try:
            response = mqtt_client.publish(topic, payload, qos=1, retain=False)
        except Exception as e:
            pytest.fail('Failed to publish results in %s (Error: %s)' % (mqtt_client.conn, e))
        else:
            time.sleep(5)
            if response[0] != 0:
                pytest.fail('There was a network error when publishing content')


def store_data(data_dir:str, data_set_file:str, send_type:str, dbms:str, anylog_conn:rest.RestCode=None, topic:str=None,
               broker:str=None, port:int=None, username:str=None, password:str= None):
    """
    Function to store data
    :args:
        data_dir:str - directory containing data
        data_set_file:str - file containing datasets to use
        send_type:str - send type [PUT, POST, MQTT]
        dbms:str - logical database name
        anylog_conn:rest.RestCode - rest connection
        topic:str - MQTT or POST topic
        broker:str - MQTT broker
        port:int - MQTT port
        username:str - MQTT user
        password:Str - MQTT password
    :params:
        data_sets:tuple - tuple of data sets to store (also used for table name(s)
        file_name:str - path of file containing data to be stored
        payloads:list - content to store in file

    """
    data_sets = ()
    data_set_file = os.path.expanduser(os.path.expandvars(data_set_file))
    if os.path.isfile(data_set_file):
        try:
            with open(data_set_file, 'r') as f:
                try:
                    data_sets = f.readlines()
                except Exception as e:
                    pytest.fail('Failed to read content in file %s (Error: %s)' % (data_set_file, e))
        except Exception as e:
            pytest.fail('Failed to open file %s (Error: %s)' % (data_set_file, e))
        else:
            for i in range(len(data_sets)):
                data_sets[i] = data_sets[i].split('\n')[0]
            data_sets = tuple(data_sets)
    else:
        pytest.fail('Failed to locate data_set file: "%s"' % data_set_file)

    for data_set in data_sets:
        for fn in os.listdir(data_dir):
            if data_set in fn:
                file_name = os.path.join(data_dir, fn)
                payloads = file_io.json_read_file(file_name=file_name)
                if send_type == 'put':
                    put_data(anylog_conn=anylog_conn, payloads=payloads, dbms=dbms, table=data_set)
                elif send_type == 'post':
                    post_data(payloads=payloads, dbms=dbms, table=table, topic=topic)
                elif send_type == 'mqtt':
                    mqtt_client = connect_mqtt_broker(broker=broker, port=port, username=username, password=password)
                    mqtt_send_data(mqtt_client=mqtt_client, topic=topic, payloads=payloads, dbms=dbms, table=table)
                time.sleep(70)

