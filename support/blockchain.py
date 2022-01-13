import os
import rest
import support
import time

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('support')[0]
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect', 'blockchain_testing')

def __read_policies()->list:
    """
    read policies from content.json
    :params:
        policies_file:str - file containing policies
        policies:list - extracted policies
    :return:
        policies - stored as POLICIES
    """
    policies = []
    policies_file = os.path.join(EXPECTED_DIR, 'content.json')
    if os.path.isfile(policies_file):
        with open(policies_file, 'r') as f:
            for policy in f.read().split('\n'):
                if policy != '':
                    policies.append(support.json_loads(policy.replace('}},', '}}')))
    return policies

# prepare_policy
def prepare_policy(anylog_conn:rest.RestCode, policy:dict):
    """
    POST `prepare policy` command against an AnyLog instance
    :args:
        anylog_conn:rest.RestClass - Rest connection to AnyLog
        policy:dict - policy to prepare
    :params:
        headers:dict - REST header
        raw_policy:str - converted policy into use-able format
    """
    raw_policy = None
    headers = {
        'command': 'blockchain prepare policy !new_policy',
        'User-Agent': 'AnyLog/1.23'
    }

    if isinstance(policy, dict):
        policy = support.json_dumps(policy)
    if isinstance(policy, str):
        raw_policy = '<new_policy=%s>' % policy

    anylog_conn.post(headers=headers, payload=raw_policy)


# get_policy_id
def get_policy_id(anylog_conn:rest.RestCode, policy_type:str)->str:
    """
    Extract policy ID from "new_policy"
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        policy_type:str - policy type to get ID for
    :params:
        headers:dict - REST header
        policy_id:str - ID of policy
    :return:
        policy_id
    """
    policy_id = None
    headers = {
        'command': 'get dictionary where format=json',
        'User-Agent': 'AnyLog/1.23'
    }

    response = anylog_conn.get(headers=headers)
    if 'new_policy' in response:
        policy = support.json_loads(response['new_policy'])
        if isinstance(policy, dict):
            policy_id = policy[policy_type]['id']

    return policy_id


def check_blockchain(anylog_conn:rest.RestCode, policy_type:str, policy_id:str=None)->bool:
    """
    validate if value in blockchain
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        policy_type:str - policy type check
        policy_id:str ID correlated to policy (type)
    :params:
        status:bool
        query:str - `blockchain get` command
        headers:dict - REST header
    :return:
        status
    """
    status = False
    query = 'blockchain get %s' % policy_type
    if policy_id is not None:
        query += ' where id=%s' % policy_id
    headers = {
        'command': query,
        'User-Agent': 'AnyLog/1.23'
    }

    if len(anylog_conn.get(headers=headers)) >= 1:
        status = True

    return status


def blockchain_get(anylog_conn:rest.RestCode, query:str)->list:
    """
    Execute blockchain get query
    :args:
        anylog_conn:rest.AnyLogCode - REST connection
        query:str - `blockchain get` command to execute
    :params:
        headers:dict - REST headers
    :return:
        results from query
    """
    headers = {
        'command': query,
        'User-Agent': 'AnyLog/1.23'
    }

    return anylog_conn.get(headers=headers)


def insert_policy(anylog_conn:rest.RestCode, policy:dict, master_node:str):
    """
    Execute POST against a blockchain policy
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        policy_type:str - policy type to get ID for
        master_node:str TCP IP & Port for TCP connection
    :params:
        headers:dict - REST header
        raw_policy:str - converted policy to be used by AnyLog
    """
    raw_policy = None
    headers = {
        'command': 'blockchain insert where policy=!new_policy and local=true and master=%s' % master_node,
        'User-Agent': 'AnyLog/1.23'
    }

    if isinstance(policy, dict):
        policy = support.json_dumps(policy)
    if isinstance(policy, str):
        raw_policy = '<new_policy=%s>' % policy

    anylog_conn.post(headers=headers, payload=raw_policy)


def declare_policies(anylog_conn:rest.RestCode, master_node:str):
    """
    Declare policies based on POLICIES in blockchain
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        master_node:str TCP IP & Port for TCP connection
    :params:
        policy_id:dict - dict of policies IDs for non-sensors
        policy_type:str - extracted policy type
        policy:dict - formatted policy
    """
    policy_id = {}
    i = 0
    for policy in __read_policies():
        policy_type = list(policy)[0]
        if policy_type == 'device':
            policy['device']['owner'] = policy_id['owner']
            policy['device']['manufacturer'] = policy_id['manufacturer']
        elif policy_type == 'sensor_type':
            policy['sensor_type']['device'] = policy_id['device']
        elif policy_type == 'sensor':
            policy['sensor']['sensor_type'] = policy_id['sensor_type']

        prepare_policy(anylog_conn=anylog_conn, policy=policy)
        policy_id[policy_type] = get_policy_id(anylog_conn=anylog_conn, policy_type=policy_type)
        if not check_blockchain(anylog_conn=anylog_conn, policy_type=policy_type, policy_id=policy_id[policy_type]):
            insert_policy(anylog_conn=anylog_conn, policy=policy, master_node=master_node)


def drop_policies(anylog_conn:rest.RestCode, master_node:str):
    """
    Drop policies from blockchain
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        master_node:str TCP IP & Port for TCP connection
    :params:
        policies:list - list of policies extracted from policy
    """
    headers = {
        'command': 'blockchain drop policy !rm_policy',
        'destination': master_node,
        'User-Agent': 'AnyLog/1.23'
    }
    for policy in list(__read_policies()):
        query = 'blockchain get %s' % list(policy)[0]
        policies = blockchain_get(anylog_conn=anylog_conn, query=query)

        for polcy in policies:
            if isinstance(polcy, dict):
                polcy = support.json_dumps(polcy)
            if isinstance(polcy, str):
                raw_policy = '<rm_policy=%s>' % polcy

            anylog_conn.post(headers=headers, payload=raw_policy)




