import os
import sys

ROOT_DIR=os.path.expandvars(os.path.expanduser(__file__)).split('tests')[0]
print(ROOT_DIR)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
SUPPORT_DIR = os.path.join(ROOT_DIR, 'support')

CONFIG_FILE = os.path.join(ROOT_DIR, 'configs', 'sample_config.ini') # will be replaced by user param

sys.path.insert(0, SUPPORT_DIR)
import file_io
import send_data
import rest_get

class TestBasicQueries:
    """The following tests basic SQL queries against AnyLog"""
    def setup(self):
        """
        Read configs and generate list of content to store
        :data sets:
            - ping_sensor
        :args:
        :params:
            self.status - whether or not able to insert data
            self.configs:dict - configurations
            self.payloads:list - content to save on AnyLog
        """
        self.status = True
        self.payloads = []
        self.configs = file_io.read_configs(config_file=CONFIG_FILE)
        self.configs['table'] = 'ping_sensor'
        for fn in os.listdir(DATA_DIR):
            if 'ping_sensor' in fn:
                file_name = os.path.join(DATA_DIR, fn)
                self.payloads += file_io.read_file(file_name=file_name)

        if self.configs['insert'] == 'true':
            self.status = send_data.store_payloads(payloads=self.payloads, configs=self.configs)

    def test_get_status(self):
        """
        Validate node is accessible
        :command:
            get status
        :assert:
            node is running
        """
        assert rest_get.get_status(conn=self.configs['conn'], username=self.configs['rest_user'],
                                   password=self.configs['rest_password']) is True