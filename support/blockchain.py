import rest
import support

POLICIES = {
    'manufacturer': { # Manufacturer of device
        'name': 'Bosch',
        'Address': 'Robert-Bosch-Platz 1 70839 Gerlingen',
        'url': 'https://www.bosch-sensortec.com/',
        'contact': 'https://www.bosch-sensortec.com/about-us/contact/',
        'phone': '+4971140040990'

    },
    'owner': {  # Owner of device (usually the customer or company generating the data)
        'name': 'New Engineering',
        'Address': '400 St Louis St, Mobile, Nevada 36602, US',
        'url': 'http://www.new-engineering.com/',
        'contact': '+12515336844'
    },
    'device': {  # device information
        'name': 'fic',
        'location': 'HQ Office',
        'owner': '',
        'manufacturer': '',
        'serial_number': 'SFb6HfZz'
    },
    # for AnyLog GUI a user can (optionally) add a layer for each type of sensor or go directly to sensor(s) policies
    # in this case we're showing having a middle layer between the device(s) and sensor(s)
    'sensor_type': {
        'name': 'fic11',
        'device': '',
        'serial_number': 'SFb6HfZz-fSAoEAvt'
    },
    # Sensor Information
    'sensor 1': {
        'sensor': {  # layer 3
            'name': 'fic11',
            'sensor_type': '',
            'serial_number': 'SFb6HfZz-fSAoEAvt-fic11'
        }
    },
    'sensor 2':{
        'sensor': {  # layer 3
            'name': 'fic11_pv',
            'sensor_type': '',
            'serial_number': 'SFb6HfZz-fSAoEAvt-fic11-pv'

        }
    },
    'sensor 3': {
        'sensor': {  # layer 3
            'name': 'fic11_mv',
            'sensor_type': '',
            'serial_number': 'SFb6HfZz-fSAoEAvt-fic11-mv'
        }
    }
}

# prepare_policy
def prepare_policy(anylog_conn:rest.RestCall, policy:dict):
    """
    POST `prepare policy` command against an AnyLog instance
    :args:
        anylog_conn:rest.RestCall - Rest connection to AnyLog
        policy:dict - policy to prepare
    :params:
        headers:dict - REST header
        raw_policy:str - converted policy into use-able format
    """
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
# post_policy
# main