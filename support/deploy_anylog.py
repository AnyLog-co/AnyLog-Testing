import os
import subprocess
import sys

slash_char = '/'
if sys.platform.startswith('win'):
    slash_char = '\\'

def deploy_anylog(anylog_api_path:str, anylog_api_config:dict):
    """
    Code to deploy AnyLog using API configs
    """
    cmd = "cd %s ; python3 %s" % (anylog_api_path, anylog_api_path + '%sdeployment%smain.py' % (slash_char, slash_char))
    cmd += " %s %s"
    for conn in anylog_api_config:
        os.system(cmd % (conn, os.path.expandvars(os.path.expanduser(anylog_api_config[conn]))))

