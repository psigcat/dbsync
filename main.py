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


def config_ini():
    
    global logger, settings, service_id, interval, sleep, default_start_tstamp, track_all_records
    global min_id, max_id, limit, delete_previous_data
    
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
             
    default_values = {'port': '1433', 'sgbd': 'mssql', 'sgbd_to': 'pgsql',  
                      'track_all_records': '1', 'process_numeric_sensors': '1', 'process_logical_sensors': '1', 
                      'min_id': '-1', 'max_id': '-1', 'limit': '-1', 'date_to': '20990101', 'delete_previous_data': '0'}             
    settings = ConfigParser.ConfigParser(default_values)
    settings.read(setting_file)
    try:
        service_id = settings.get('main', 'service_id')    
        interval = settings.get('main', 'interval')    
        sleep = settings.get('main', 'sleep')    
        default_start_tstamp = settings.get('main', 'default_start_tstamp')    
        track_all_records = settings.get('main', 'track_all_records')    
        min_id = settings.get('main', 'min_id')    
        max_id = settings.get('main', 'max_id')    
        limit = settings.get('main', 'limit')    
        delete_previous_data = settings.get('main', 'delete_previous_data')    
        interval = float(interval)
        sleep = int(sleep)
        default_start_tstamp = int(default_start_tstamp)
        track_all_records = int(track_all_records)
        min_id = int(min_id)
        max_id = int(max_id)
        limit = int(limit)
        delete_previous_data = int(delete_previous_data)
    except ConfigParser.NoOptionError, e:
        logger.warning('{config_ini} %s' % e)
        return False
    except ValueError, e:
        logger.warning('{config_ini} %s' % e)
        return False
    
    return True
    

def check_param_numeric(param):  
    ''' Check parameter. Return True if is numeric '''
    valid = True  
    if not isNumber(param):
        logger.warning("Parameter '"+param+"' must be numeric. Please check file dbsync.config")
        valid = False
    return valid
        
    
def connect_databases():
    ''' Connect to Databases origin and destination '''
    
    global db_from, db_dest, host, port, db, user, pwd, sgbd

    # DB origin. Connect to local Database (by default MsSQL)
    try:
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
    except ConfigParser.NoOptionError, e:
        logger.warning('{connect_databases} %s' % e)
        return False

    # DB destination. Connect to remote Database (by default PostgreSQL)
    try:
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
    except ConfigParser.NoOptionError, e:
        logger.warning('{connect_databases} %s' % e)
        return False        

    return (from_ok and dest_ok)


def set_task():
    ''' Sets a new task '''
    
    task = db_task.DbTask()
    task.set_database_params(host, port, db, user, pwd, sgbd)
    task.set_main_params(service_id, interval, sleep, default_start_tstamp, track_all_records)
    task.set_db_to(db_dest)
    task.set_settings(settings)
    
    # Execute main task
    task.copy_data(min_id, max_id, limit, delete_previous_data)              
    
    
if __name__ == '__main__':
    main()
    
