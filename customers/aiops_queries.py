

import requests
from requests.exceptions import HTTPError

# -----------------------------------------------------------------------------------
# GET request
# -----------------------------------------------------------------------------------
def do_get(url, command, destination = None):

    headers_data = {
        'command' : command,
        'User-Agent': "AnyLog/1.23"
    }
    if destination:
        headers_data['destination'] = destination

    try:
        response = requests.get(url=url, params=None, verify=False, headers=headers_data, timeout=10)
    except HTTPError as http_err:
        error_msg = "REST GET HTTP Error from %s Error: %s" % (str(url), str(http_err))
        response = None
    except Exception as err:
        error_msg = "REST GET Error: %s" % str(err)
        response = None
    else:
        error_msg = None

    if error_msg:
        print(error_msg)

    return response

def process_response(response, query_id, print_details, query_description):
    '''
    print the reply, or the reply status
    '''
    if response:
        if print_details:
            print("\nQuery %u - %s:\n" % (query_id, query_description))
            print(response.text)
        else:
            print("\nQuery %u: Success: %s" % (query_id, query_description))
    else:
        print("\nQuery %u: Failed: %s" % (query_id, query_description))
        exit(-1)


def run_commands(url, query_id, print_details):

    '''
    The list of aiops commands to test
    '''

    response = do_get(url, "get status")
    process_response(response, 1, print_details, "Get status")


    if not query_id or query_id == 1:
        response = do_get(url, "get tables where dbms = *")
        process_response(response, 1, print_details, "get tables where dbms = *")

    if not query_id or query_id == 2:
        response = do_get(url, "get columns where dbms = aiops and table = flow_in1")
        process_response(response, 2, print_details, "List columns in table")

    if not query_id or query_id == 3:
        response = do_get(url, "sql aiops select insert_timestamp, timestamp, value from flow_in1 limit 10", destination='network')
        process_response(response, 3, print_details, "10 random rows")


    if not query_id or query_id == 4:
        response = do_get(url, "sql aiops SELECT increments(minute, 1, timestamp), MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value), COUNT(*) FROM fic12_fb_factualvalue WHERE timestamp >= '2021-05-30 23:59:59' AND timestamp <= '2021-06-02 20:15:00'", destination='network')
        process_response(response, 4, print_details, "increments")



    if not query_id or query_id == 5:
        response = do_get(url, "sql aiops SELECT MIN(timestamp), MAX(timestamp), MIN(value), AVG(value), MAX(value) FROM fic12_fb_factualvalue WHERE period(day, 1, NOW(), timestamp)" , destination='network')
        process_response(response, 5, print_details, "period")


    if not query_id or query_id == 6:
        response = do_get(url, "sql aiops select insert_timestamp, timestamp, value  from flow_in1 order by timestamp desc limit 10", destination='network')
        process_response(response, 6, print_details, "desc by timestamp")

    if not query_id or query_id == 7:
        response = do_get(url, "sql aiops select max(timestamp) from flow_in1", destination='network')
        process_response(response, 7, print_details, " max(timestamp)")



def tests():
    #url = "http://10.0.0.78:7849"  #
    #url = "http://20.97.12.66:2049"         # MAIN AI-OPS
    url = "http://13.67.180.124:2149"      # TESTNET  AI-OPS

    query_id = 7
    print_details = True
    loop_counter = 1       # 0 will run forever

    i = 0
    while i < loop_counter:
        run_commands(url, query_id, print_details)
        if loop_counter:
            i += 1


if __name__ == '__main__':
    tests()