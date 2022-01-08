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
import rest

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


def store_payloads(send_type:str, payloads:list, dbms:str, table:str, anylog_conn:rest.RestCode=None,
                   topic:str=None, broker:str=None, port:int=None, username:str=None, password:str=None):
    """
    Send payloads into either AnyLog or MQTT broker based on configs
    :args:
        send_type:str - format to send data (PUT, POST, MQTT)
        payloads:list - content to store
        dbms:str - logical database name
        table:str - tale to store data in
        anylog_conn:rest.RestClass - connection to REST class (used by PUT / POST)
        topic:str - topic for either POST or MQTT
        broker:str - MQTT broker
        port:int - MQTT port
        username:str - MQTT user
        password:MQTT password
    """
    if send_type == 'put':
        anylog_conn.put(payloads=payloads, dbms=dbms, table=table)
    elif send_type == 'post':
        anylog_conn.post(payloads=payloads, dbms=dbms, table=table, topic=topic)
    elif send_type == 'mqtt':
        mqtt_client = connect_mqtt_broker(broker=broker, port=port, username=username, password=password)
        mqtt_send_data(mqtt_client=mqtt_client, topic=topic, payloads=payloads, dbms=dbms, table=table)
    time.sleep(70)