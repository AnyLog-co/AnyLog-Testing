import datetime
import filecmp
import os
import pytest
import sys
import time

import tests.pytest_setup_teardown as pytest_setup_teardown
import tests.execute_tests as execute_tests

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

CONFIG_FILE = os.path.join(ROOT_DIR, 'configs', 'sample_config.ini') # will be replaced by user param
EXPECTED_DIR = os.path.join(ROOT_DIR, 'expect', 'blockchain_testing')
ACTUAL_DIR = os.path.join(ROOT_DIR, 'actual', 'blockchain_testing')

sys.path.insert(0, SUPPORT_DIR)
import blockchain
import file_io
import send_data
import support


class TestBlockchain:
    def setup_class(self):
        """
        Read configs and generate list of content to store
        :data sets:
            - ping_sensor
        :args:
        :params:
            self.status - whether or not able to access AnyLog
            self.configs:dict - configurations
            payloads:list - content to save on AnyLog
        """
        self.status, self.configs, self.anylog_conn = pytest_setup_teardown.setup_code(config_file=CONFIG_FILE,
                                                                                       expected_dir=EXPECTED_DIR,
                                                                                       actual_dir=ACTUAL_DIR)
        if self.status is False:
            pytest.fail(f"Failed to get status against AnyLog node {self.configs['conn']}")

        blockchain.declare_policies(anylog_conn=self.anylog_conn, master_node=self.configs['master_node'])

    def teardown_class(self):
        """
        Remove policies from blockchain
        """
        blockchain.drop_policies(anylog_conn=self.anylog_conn, master_node=self.configs['master_node'])

    def test_validate_manufacturer(self):
        """
        Test whether or not manufacturer was added
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get manufacturer
        :assert:
            manufacturer is returned
        """
        query = "blockchain get manufacturer"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_validate_manufacturer.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_validate_manufacturer.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_validate_owner(self):
        """
        Test whether or not owner was added
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get owner
        :assert:
            owner is returned
        """
        query = "blockchain get owner"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_validate_owner.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_validate_owner.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_validate_device(self):
        """
        Test whether or not owner was added
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get device
        :assert:
            device is returned
        """
        query = "blockchain get device"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_validate_device.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_validate_device.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_validate_sensor_type(self):
        """
        Test whether or not sensor_type was added
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get sensor_type
        :assert:
            sensor_type is returned
        """
        query = "blockchain get sensor_type"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_validate_sensor_type.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_validate_sensor_type.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_validate_sensor(self):
        """
        Test whether or not sensor was added
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get sensor
        :assert:
            sensor is returned
        """
        query = "blockchain get sensor"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_validate_sensor.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_validate_sensor.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_get_sensor_by_id(self):
        """
        get sensor by ID
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get sensor where id=72f68be51c87a100a033623f8a3c1f1d
        :assert:
            sensor with ID 72f68be51c87a100a033623f8a3c1f1d returned
        """
        query = "blockchain get sensor where id=97ac750a8bcd175cdc1bc862df834bd9"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_get_sensor_by_id.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_get_sensor_by_id.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_bring_count(self):
        """
        Check row count with bring.count
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get sensor bring.count
        :assert:
            there are 3 sensors
        """
        query = "blockchain get sensor bring.count"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_bring_count.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_bring_count.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_bring_unique(self):
        """
        Check row count with bring.unique
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get sensor bring.unique
        :assert:
            there are 3 sensors
        """
        query = "blockchain get sensor bring.unique"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23'
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_bring_unique.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_bring_unique.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_manufacturer_extract_id(self):
        """
        Extract ID of manufacturer from blockchain
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results
        :query:
            blockchain get manufacturer bring [manufacturer][id]
        :assert:
            manufacturer ID is returned
        """
        query = "blockchain get manufacturer where name=Bosch bring [manufacturer][id]"
        headers = {
            'command': query,
            'User-Agent': 'AnyLog/1.23',
        }
        expected_file = os.path.join(EXPECTED_DIR, 'test_manufacturer_extract_id.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_manufacturer_extract_id.rslts')
        if self.status is True:
            execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                             expected_file=expected_file, actual_file=actual_file)
        else:
            pytest.fail('Failed to validate connection to AnyLog')

    def test_where_manufacturer_id(self):
        """
        Test using where condition that's no ID correlated to the policy
        :params:
            headers:dict - REST headers
            expected_file:str - file containing expected results
            actual_file:str - file updated with results

        """
        manufacturer_id = None
        # extract manufacturer from file
        manufacturer_file = os.path.join(EXPECTED_DIR, 'test_manufacturer_extract_id.rslts')
        expected_file = os.path.join(EXPECTED_DIR, 'test_where_manufacturer_id.rslts')
        actual_file = os.path.join(ACTUAL_DIR, 'test_where_manufacturer_id.rslts')
        if os.path.isfile(manufacturer_file):
            try:
                with open(manufacturer_file, "r") as f:
                    try:
                        manufacturer_id = f.readlines()[1].split('\n')[-1]
                    except Exception as e:
                        pytest.fail('Failed to extract manufacturer ID from file: %s (Error: %s)' % (manufacturer_file, e))
            except Exception as e:
                pytest.fail('Failed to read file %s (Error: %s)' % (manufacturer_file, e))
        else:
            pytest.fail('Unable to locate file: %s' % manufacturer_file)

        if manufacturer_id is not None and manufacturer_id != '':
            query = "blockchain get * where manufacturer=%s" % manufacturer_id
            headers = {
                'command': query,
                'User-Agent': 'AnyLog/1.23',
            }
            if self.status is True:
                execute_tests.execute_blockchain(anylog_conn=self.anylog_conn, headers=headers, query=query,
                                                 expected_file=expected_file, actual_file=actual_file)
            else:
                pytest.fail('Failed to validate connection to AnyLog')

    @pytest.mark.skip('inconsistent behavior')
    def test_drop_policy(self):
        """
        Validate `DROP policy` works
        :params:
            headers:dict - REST headers
            responses:list - list of results from REST request
        :assert:
            validate that at the end of the process 'owner' is no longer a policy on the blockchain
        :note:
            due to the complexity of the test, there's no correlating results file
        """
        # Extract list of owners from blockchain
        headers = {
            'command': 'blockchain get owner',
            'User-Agent': 'AnyLog/1.23'
        }
        responses = self.anylog_conn.get(headers)

        # remove policy(s) from blockchain - note wait time is 60 seconds at the end due to blockchain sync
        if isinstance(responses, list) and len(responses) != 0:
            headers = {
                'command': 'blockchain drop policy !rm_policy',
                'destination': self.configs['master_node'],
                'User-Agent': 'AnyLog/1.23'
            }
            for response in responses:
                if isinstance(response, dict):
                    response = support.json_dumps(response)
                if isinstance(response, str):
                    raw_policy = '<rm_policy=%s>' % response
                self.anylog_conn.post(headers=headers, payload=raw_policy)
            time.sleep(60)

        # validate owner was removed
        headers = {
            'command': 'blockchain get owner',
            'User-Agent': 'AnyLog/1.23'
        }
        responses = self.anylog_conn.get(headers)
        assert responses == '' or len(responses) == 0