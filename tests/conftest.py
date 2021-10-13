import os
import sys

CONFIG_FILE = '$HOME/AnyLog-Testing/config/default_config.ini'

slash_char = '/'
if sys.platform.startswith('win'):
    slash_char = '\\'

ROOT_DIR = os.path.expanduser(os.path.expandvars('$HOME%sAnyLog-Testing' % slash_char))


def pytest_addoption(parser):
    """Add the --user option"""
    parser.addoption('--root-dir', action='store', default=ROOT_DIR, help='AnyLog-Testing path')
    parser.addoption('--config-file', action='store', default=ROOT_DIR + slash_char + 'config' + slash_char + 'default_config.ini',
                     help='config file for test(s)')

option = None
config_file = None

def pytest_configure(config):
    """Make cmdline arguments available to dbtest"""
    global option
    option = config.option
