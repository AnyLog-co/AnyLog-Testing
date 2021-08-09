import datetime

def convert_timezone(query:str, timestamp:str)->str: 
    """
    Convert timestamp to UTC 
    :args: 
        query:str - query executed
        timestamp:str - timestamp to convert
    :params: 
        dt_timestamp:datetime.datetime - convert str to datetime
        utc_timestamp:str - converted timestamp
    """
    utc_timestamp = timestamp
    try: 
        dt_timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        assert True == False, "Failed to convert '%s' to datetime.datetime (Error: %s).\n\tQuery: %s\n" % (timestamp, e, query)
        dt_timestam = None 

    if dt_timestamp != None:
        try: 
            dt_utc_timestamp = dt_timestamp.replace(tzinfo=datetime.timezone.utc)
        except Exception as e: 
            assert True == False, "Failed to update timezone (Error: %s).\n\tQuery: %s\n" % (e, query)

        else: 
            try:
                utc_timestamp = dt_utc_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
            except Exception as e: 
                assert True == False, "Failed to convert datetime to string after converting to UTC. (Error: %s).\n\tQuery: %s\n" % (e, query)


        return utc_timestamp

