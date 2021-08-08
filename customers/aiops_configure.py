

import requests
from requests.exceptions import HTTPError


# -----------------------------------------------------------------------------------
# Post request
# -----------------------------------------------------------------------------------
def do_post(url, command, data_str, data_json, destination = None):

    headers_data = {
        'command' : command,
        'User-Agent': "AnyLog/1.23"
    }
    if destination:
        headers_data['destination'] = destination

    try:
        response = requests.post(url=url, headers=headers_data, data=data_str, json=data_json, verify=False)
    except HTTPError as http_err:
        error_msg = "REST POST HTTPError Error: %s" % str(http_err)
        response = None
    except Exception as err:
        error_msg = "REST POST Error: %s" % str(err)
        response = None
    else:
        error_msg = None

    return [response, error_msg]


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


def run_commands(url, target_node, print_details):

    '''
    The list of aiops commands to test
    '''

    response = do_get(url, "get status")
    process_response(response, 1, print_details, "Get status")

    # run client  76.214.179.41:2050 get status

    commands_list =   ["schedule name = disk_space and time = 15 seconds task disk_space = get disk percentage .",
                     "schedule name = cpu_percent and time = 15 seconds task cpu_percent = get node info cpu_percent",
                     "schedule name = get_operator_stat and time = 15 seconds task operator_stat = get operator stat format = json",
                     "task remove where scheduler = 1 and name = monitor_operators",
                     "schedule name = monitor_operators and time = 15 seconds task run client 24.23.250.144:7848 monitor operators where info = !!operator_stat"]

    for command in commands_list:
        response, err_msg = do_post(url, command, None, None, target_node)

def tests():
    url = "http://10.0.0.78:7849"  #
    #url = "http://20.97.12.66:2049"         # MAIN AI-OPS
    #url = "http://13.67.180.124:2149"       # TESTNET  AI-OPS

    target_node = "76.214.179.41:2050"

    print_details = True

    run_commands(url, target_node, print_details)


if __name__ == '__main__':
    tests()