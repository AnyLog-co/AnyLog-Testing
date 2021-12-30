import datetime
import json
import pytz
import random
import tzlocal


def json_dumps(data:dict)->str:
    """
    Convert dictionary to string
    :args:
        data:dict - data to convert
    :return:
        converted data, if fails return original data
    """
    try:
        return json.dumps(data)
    except:
        return data


def json_loads(data:str)->dict:
    """
    Convert dictionary to dict
    :args:
        data:str - data to convert
    :return:
        converted data, if fails return original data
        """
    try:
        return json.loads(data)
    except:
        return data


def extract_values(payloads:list, values_column:str='value')->list:
    """
    Extract value(s) form payalods
    :args:
        payloads:list - content sent to be stored
        values_column:str - json key to extract data from
    :params:
        values:list - list of values
    :return:
        values
    """
    values = []
    if values_column == 'timestamp':
        for payload in payloads:
            try:
                values.append(datetime.datetime.strftime(payload['timestamp'], '%Y-%m-%d %H:%M:%S.%f'))
            except:
                pass
    else:
        for payloads in payloads:
            values.append(payloads[values_column])

    return values