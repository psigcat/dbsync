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
    
    global logger, settings, scada_id, interval, sleep, default_start_tstamp, time_gap
    
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
             
    settings = ConfigParser.ConfigParser({'sgbd': 'mssql', 'sgbd_to': 'pgsql', 'time_gap': '-1'})
    settings.read(setting_file)
    scada_id = settings.get('main', 'scada_id')    
    interval = settings.get('main', 'interval')    
    sleep = settings.get('main', 'sleep')    
    default_start_tstamp = settings.get('main', 'default_start_tstamp')    
    time_gap = settings.get('main', 'time_gap')    
    if not check_param_numeric(scada_id):
        return False    
    if not check_param_numeric(interval):
        return False
    if not check_param_numeric(sleep):
        return False
    if not check_param_numeric(default_start_tstamp):
        return False
    if not check_param_numeric(time_gap):
        return False
    
    interval = float(interval)
    sleep = int(sleep)
    default_start_tstamp = int(default_start_tstamp)
    time_gap = int(time_gap)
    
    return True
    

def check_param_numeric(param):  
    
    valid = True  
    if not isNumber(time_gap):
        logger.warning("Parameter '"+param+"' must be numeric. Please check file dbsync.config")
        valid = False
    return valid
        
    
def connect_databases():

    global db_from, db_dest

    # DB origin. Connect to local Database (by default MsSQL)
    host = settings.get('database', 'host')
    port = settings.get('database', 'port')
    db = settings.get('database', 'db')
    user = settings.get('database', 'user')
    pwd = settings.get('database', 'pwd')
    sgbd = settings.get('database', 'sgbd')
    if sgbd.lower() == 'mssql':
        db_from = MsSqlDao()
    else:
        db_from = PgDao()     
    db_from.set_params(host, port, db, user, pwd)
    from_ok = db_from.init_db()

    # DB destination. Connect to remote Database (by default PostgreSQL)
    host_to = settings.get('database', 'host_to')
    port_to = settings.get('database', 'port_to')
    db_to = settings.get('database', 'db_to')
    user_to = settings.get('database', 'user_to')
    pwd_to = settings.get('database', 'pwd_to')
    sgbd_to = settings.get('database', 'sgbd_to')    
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
    task.set_params(scada_id, interval, sleep, default_start_tstamp, time_gap)
    task.set_db_to(db_dest)
    
    # Execute main task
    task.copy_data()              
        

if __name__ == '__main__':
    main()