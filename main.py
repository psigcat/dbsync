# -*- coding: utf-8 -*-
from PyQt4.QtSql import *     # @UnusedWildImport
import sys
import os.path
import ConfigParser

if 'db_task' in sys.modules:
    del sys.modules["db_task"]
if 'utils' in sys.modules:
    del sys.modules["utils"]
if 'dao.pg_dao' in sys.modules:
    del sys.modules["dao.pg_dao"]
if 'dao.mssql_dao' in sys.modules:
    del sys.modules["dao.mssql_dao"]

import db_task
from utils import *  # @UnusedWildImport
from dao.pg_dao import PgDao
from dao.mssql_dao import MsSqlDao


def main():

    if not config_ini():
        return
    if not connect_databases():
        return
    set_task()


def config_ini():
    
    global settings, scada_id, interval, sleep
    
    # Load local settings of the plugin
    cur_dir = os.path.dirname(__file__)
    setting_file = os.path.join(cur_dir, 'config', 'dbsync.config')
    if not os.path.isfile(setting_file):
        print "Config file not found at: "+setting_file
        return False
             
    settings = ConfigParser.ConfigParser()
    settings.read(setting_file)
    scada_id = settings.get('main', 'scada_id')    
    interval = settings.get('main', 'interval')    
    sleep = settings.get('main', 'sleep')    
    if not isNumber(scada_id):
        print "Parameter 'scada_id' must be numeric. Please check file dbsync.config"
        return False    
    if not isNumber(interval):
        print "Parameter 'interval' must be numeric. Please check file dbsync.config"
        return False
    if not isNumber(sleep):
        print "Parameter 'sleep' must be numeric. Please check file dbsync.config"
        return False
    interval = float(interval)
    sleep = int(sleep)
    
    return True
    
    
def connect_databases():

    global db_from, db_dest

    # Connect to local Database
    host = settings.get('main', 'host')
    port = settings.get('main', 'port')
    db = settings.get('main', 'db')
    user = settings.get('main', 'user')
    pwd = settings.get('main', 'pwd')
    db_from = MsSqlDao()
    db_from.set_params(host, port, db, user, pwd)
    if not db_from.init_db():
        return False

    # Connect to remote Database
    host_to = settings.get('main', 'host_to')
    port_to = settings.get('main', 'port_to')
    db_to = settings.get('main', 'db_to')
    user_to = settings.get('main', 'user_to')
    pwd_to = settings.get('main', 'pwd_to')
    db_dest = PgDao()
    db_dest.set_params(host_to, port_to, db_to, user_to, pwd_to)
    if not db_dest.init_db():
        return False

    return True


def set_task(threading = True):

    task = db_task.DbTask()
    task.set_scada_id(scada_id)
    task.set_interval(interval)
    task.set_sleep(sleep)
    task.set_db_from(db_from)
    task.set_db_to(db_dest)
    
    if threading:
        task.truncate_log_detail()
        task.copy_data()
#         task.copy_data_sensor(8)
    else:
        task.job_copy_data()                    
        

if __name__ == '__main__':
    main()