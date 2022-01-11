import argparse
import filecmp
import os
import pytest_setup_teardown
try:
    import termcolor
except:
    pass
import sys

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
ACTUAL_DIR = os.path.join(ROOT_DIR, 'actual')

SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')
sys.path.insert(0, SUPPORT_DIR)
import file_io
import send_data
import rest



def execute_query(anylog_conn:rest.RestCode, dbms:str, query:str, expected_results:str=None, results_dir:str=ACTUAL_DIR,
                  network:bool=False)->bool:
    """
    Execute query & store results
    :args:
        anylog_conn:rest.RestCode - connection to AnyLog
        dbms:str - logical databases to query against
        query:str - query to execute
        expected_results:str - results to compare against
        results:str - results directory
        network:bool - whether or not to execute query over network
    :params:
        status:bool
        actual_file:str - file that'll store the results
        headers:dict - REST header
        response:dict - "raw" response from AnyLog
        results:list - content to write to file
    :return:
        status, actual_file
    """
    status = False
    actual_file = file_io.generate_file_name(results_dir=results_dir)

    headers = {
        'command': query % dbms,
        'User-Agent': 'AnyLog/1.23'
    }

    if network is True:
        headers['destination'] = 'network'

    response = anylog_conn.get(headers=headers)
    if isinstance(response, dict):
        try:
            results = response['Query']
        except Exception as e:
            if 'err_code' in output and 'err_text' in output:
                print("Failed to extract results from 'COUNT(*)' (Error Code: %s | Error: %s)" % (
                    response['err_code'], response['err_text']))
            else:
                print("Failed to extract results from 'COUNT(*)' (Error: %s)" % e)
        else:
            if file_io.write_file(query=query,file_name=actual_file, results=results) is True:
               status = filecmp.cmp(actual_file, expected_results)

    return status, actual_file


def main():
    """
    The following tool is intended to allow for executing AnyLog
    :positional arguments:
        configs     configs file
        tests_dir   directory containing files with results
    :optional arguments:
        -h, --help           show this help message and exit
        --network [NETWORK]  Whether or not to execute tests across the network
    :params:
        status:bool
        config_file:str - configuration file
        tests_dir:str - directory containing files with results
        configs:dict - configs from configuration file
        anylog_conn:rest.RestCode - REST connection
        summary:dict - counter for success / fail
    :output:
        Query: %s | Status: %s [| Expected File: %s | Actual File: %s]
        Summary
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('configs', type=str, default='$HOME/AnyLog-Testing/configs/sample_config.ini', help='configs file')
    parser.add_argument('tests_dir', type=str, default='$HOME/AnyLog-Testing/expect', help='directory containing files with results')
    parser.add_argument('data_dir', type=str, default='$HOME/AnyLog-Testing/data', help='directory containing JSON data to be used')
    parser.add_argument('--network', type=bool, nargs='?', const=True, default=False, help='Whether or not to execute tests across the network')
    args = parser.parse_args()

    config_file = os.path.expandvars(os.path.expanduser(args.configs))

    if not os.path.isfile(config_file):
        print('Failed to locate configuration file')
        exit(1)

    tests_dir = os.path.expandvars(os.path.expanduser(args.tests_dir))
    if not os.path.isdir(tests_dir):
        print('Failed to locate tests directory')
        exit(1)

    data_dir = os.path.expandvars(os.path.expanduser(args.data_dir))
    if not os.path.isdir(data_dir):
        print('Failed to locate data directory')
        exit(1)

    summary = {
        'total': 0,
        'failed': 0,
        'success': 0
    }

    status, configs, anylog_conn = pytest_setup_teardown.setup_code(config_file=config_file, expected_dir=tests_dir,
                                                                    actual_dir=ACTUAL_DIR)
    if status is False:
        print('Failed to get status against AnyLog node %s' % configs['conn'])

    # Insert data process
    if configs['insert'] == 'true':
        data_set_file = os.path.join(tests_dir, 'data_set.txt')
        send_data.store_data(data_dir=data_dir, data_set_file=data_set_file, send_type=configs['send'],
                             dbms=configs['dbms'], anylog_conn=anylog_conn, topic=configs['topic'],
                             broker=configs['broker'], port=configs['port'],
                             username=configs['mqtt_user'], password=configs['mqtt_password'])

    for fn in os.listdir(tests_dir):
        full_fn = os.path.join(tests_dir, fn)
        # extract query
        if '.txt' not in full_fn:
            with open(full_fn, 'r') as f:
                query = f.readlines()[0].split('\n')[0]
            status, actual_file = execute_query(anylog_conn=anylog_conn, dbms = configs['dbms'], query=query,
                                                expected_results=full_fn, results_dir=ACTUAL_DIR, network=True)

            summary['total'] += 1
            if status is True:
                try:
                    stmt = 'Query: %s | Status: %s' % (query, termcolor.colored('Success', attrs=['bold']))
                except Exception:
                    stmt = 'Query: %s | Status: %s' % (query, 'Success')
                summary['success'] += 1
            elif status is False:
                try:
                    stmt = 'Query: %s | Status: %s | Expect file: %s | Actual file: %s' % (query,
                                                                                           termcolor.colored('FAILED', 'red',
                                                                                                             attrs=['bold']),
                                                                                           full_fn, actual_file)
                except Exception:
                    stmt = 'Query: %s | Status: %s | Expect file: %s | Actual file: %s' % (query,'FAILED', full_fn, actual_file)
                summary['failed'] += 1

            print(stmt)

    # print summary
    success = str(round(summary['success'] / summary['total'] * 100, 2)) + '%'
    failed = str(round(summary['failed'] / summary['total'] * 100, 2)) + '%'
    print("\nTotal: %s | Success: %s | Failed: %s" % (summary['total'], success, failed))


if __name__ == '__main__': 
    main() 