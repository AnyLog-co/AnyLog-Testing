import datetime
from dateutil import tz


def convert_timezone(query:str, timestamp:str)->str: 
    """
    Convert timestamp to UTC 
    :args: 
        query:str - query executed
        timestamp:str - timestamp to convert
    :params: 
        subseconds:str - extracred sub-seconds
        timestamp:str - timestamp w/o sub-seconds
        local_time:datetime.datetime - timestamp as datetime format
        utc_local_timme:datetime.datetime - timestamp as local time
        utc_time:str - string format of utc_local_time
    :return:
        utc_time
    """
    # remove sub-seconds
    subseconds = timestamp.split('.')[-1]
    timestamp = timestamp.split('.')[0]

    try:
        local_time = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        assert True == False, 'Failed to convert string to datetime (Error: %s).\n\tQuery: %s\n' % (e, query)

    try:
        utc_local_time = local_time.astimezone(tz.tzutc())
    except Exception as e:
        assert True == False, 'Failed to convert datetime back to string (Error: %s).\n\tQuery: %s\n' % (e, query)

    try:
        utc_time = utc_local_time.strftime('%Y-%m-%d %H:%M:%S') + '.%s' % subseconds
    except Exception as e:
        assert True == False, 'Failed to convert datetime back to string (Error: %s).\n\tQuery: %s\n' % (e, query)

    return utc_time