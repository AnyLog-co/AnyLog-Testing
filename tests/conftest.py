import os
CONFIG_FILE = '$HOME/AnyLog-Testing/config/default_config.ini'

def pytest_addoption(parser):
    """Add the --user option"""
    parser.addoption('--config-file', action='store', default=CONFIG_FILE, help='config file for test(s)')

option = None
config_file = None

def pytest_configure(config):
    """Make cmdline arguments available to dbtest"""
    global option
    option = config.option
