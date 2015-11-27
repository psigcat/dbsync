# -*- coding: utf-8 -*-
import os.path
import ConfigParser

import sys
if 'db_task' in sys.modules:
    del sys.modules["db_task"]
if 'utils' in sys.modules:
    del sys.modules["utils"]
if 'dao.pg_dao' in sys.modules:
    del sys.modules["dao.pg_dao"]
if 'dao.mssql_dao' in sys.modules:
    del sys.modules["dao.mssql_dao"]

from dao.pg_dao import PgDao
from dao.mssql_dao import MsSqlDao
from utils import *  # @UnusedWildImport
import db_task


def main():

    if not config_ini():
        return
    if not connect_databases():
        return
    set_task()
    
    logger.info('App finished\n')    


def config_ini():
    
    global logger, settings, scada_id, interval, sleep, default_start_tstamp
    
    # Set daily log file
    app_name = 'dbsync'
    set_logging('log', app_name)
    logger = logging.getLogger(app_name)
    logger.info('App started')

    # Load local settings of the plugin
    cur_dir = os.path.dirname(__file__)
    setting_file = os.path.join(cur_dir, 'config', app_name+'.config')
    if not os.path.isfile(setting_file):
        logger.warning("Config file not found at: "+setting_file)
        return False
             
    settings = ConfigParser.ConfigParser({'sgbd': 'mssql', 'sgbd_to': 'pgsql'})
    settings.read(setting_file)
    scada_id = settings.get('main', 'scada_id')    
    interval = settings.get('main', 'interval')    
    sleep = settings.get('main', 'sleep')    
    default_start_tstamp = settings.get('main', 'default_start_tstamp')    
    if not isNumber(scada_id):
        logger.warning("Parameter 'scada_id' must be numeric. Please check file dbsync.config")
        return False    
    if not isNumber(interval):
        logger.warning("Parameter 'interval' must be numeric. Please check file dbsync.config")
        return False
    if not isNumber(sleep):
        logger.warning("Parameter 'sleep' must be numeric. Please check file dbsync.config")
        return False
    if not isNumber(default_start_tstamp):
        logger.warning("Parameter 'default_start_tstamp' must be numeric. Please check file dbsync.config")
        return False
    interval = float(interval)
    sleep = int(sleep)
    default_start_tstamp = int(default_start_tstamp)
    
    return True
    
    
def connect_databases():

    global db_from, db_dest

    # DB origin. Connect to local Database (by default MsSQL)
    host = settings.get('main', 'host')
    port = settings.get('main', 'port')
    db = settings.get('main', 'db')
    user = settings.get('main', 'user')
    pwd = settings.get('main', 'pwd')
    sgbd = settings.get('main', 'sgbd')
    if sgbd.lower() == 'mssql':
        db_from = MsSqlDao()
    else:
        db_from = PgDao()     
    db_from.set_params(host, port, db, user, pwd)
    from_ok = db_from.init_db()

    # DB destination. Connect to remote Database (by default PostgreSQL)
    host_to = settings.get('main', 'host_to')
    port_to = settings.get('main', 'port_to')
    db_to = settings.get('main', 'db_to')
    user_to = settings.get('main', 'user_to')
    pwd_to = settings.get('main', 'pwd_to')
    sgbd_to = settings.get('main', 'sgbd_to')    
    if sgbd_to.lower() == 'mssql':
        db_dest = MsSqlDao()
    else:
        db_dest = PgDao()      
    db_dest.set_params(host_to, port_to, db_to, user_to, pwd_to)
    dest_ok = db_dest.init_db()

    return (from_ok and dest_ok)


def set_task():

    task = db_task.DbTask()
    task.set_settings(settings)
    task.set_db_parameters()
    task.set_scada_id(scada_id)
    task.set_interval(interval)
    task.set_default_start_tstamp(default_start_tstamp)
    task.set_sleep(sleep)
    task.set_db_to(db_dest)
    
    # Execute main task
    task.copy_data()             
    #task.copy_data(delete_previous_data=True, min_id=6, max_id=22)             
        

if __name__ == '__main__':
    main()