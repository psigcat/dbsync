import time
import logging
import threading

from dao.pg_dao import PgDao
from dao.mssql_dao import MsSqlDao

import lib.schedule as schedule
from utils import *  # @UnusedWildImport


class DbTask():

    def __init__(self):
        self.logger = logging.getLogger('dbsync') 
        self.sensor_dates = {} 
        
    def set_settings(self, settings):
        self.settings = settings
    
    def set_db_parameters(self):
        self.host = self.settings.get('main', 'host')
        self.port = self.settings.get('main', 'port')
        self.db = self.settings.get('main', 'db')
        self.user = self.settings.get('main', 'user')
        self.pwd = self.settings.get('main', 'pwd')
        self.sgbd = self.settings.get('main', 'sgbd')
        
    def set_scada_id(self, scada_id):
        self.scada_id = scada_id
        
    def set_interval(self, interval):
        self.interval = interval
        
    def set_default_start_tstamp(self, default_start_tstamp):
        self.default_start_tstamp = default_start_tstamp
        
    def set_sleep(self, sleep):
        self.sleep = sleep

    def set_db_to(self, db):
        self.db_to = db
       
    def run_threaded(self, *args):
        job_thread = threading.Thread(target=args[0], args=(args[1],))
        job_thread.start()
        
        
    def create_thread_conn(self):        
               
        # DB origin. Connect to local Database (by default MsSQL)
        if self.sgbd.lower() == 'mssql':
            db_from = MsSqlDao()
        else:
            db_from = PgDao()          
        db_from.set_params(self.host, self.port, self.db, self.user, self.pwd)
        ok = db_from.init_db()  
        if ok:
            return db_from 
        else:         
            return None


    # Get last row inserted for the selected scada and sensor
    def get_last_row(self, scada, sensor):
        
        sql = "SELECT MAX(last_date) FROM audit.log_detail"
        sql = sql + " WHERE scada_id = "+str(scada)+" AND sensor_id = "+str(sensor)   
        result = None
        try:
            row = self.db_to.get_row(sql)
            result = row[0]
        except Exception as e:
            self.logger.warning('{get_last_row} Error %s' % e)
        finally:
            if result is None:
                result = self.default_start_tstamp
            return str(result)
                
        
    def job_copy_data(self, sensor_id):

        self.logger.info("{job_copy_data} Sensor "+str(sensor_id)+" - Start "+str(threading.current_thread()))
 
        # Get id of the last record inserted
        previous_date = self.sensor_dates[sensor_id]
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("{job_copy_data} Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database
        # TODO: Origin sensor table hardcoded
        schema_from = "dbo"
        table_from = "LineStep_"+str(sensor_id)
        exists = db_from.check_table(schema_from, table_from)
        if not exists:
            self.logger.info("{job_copy_data} Sensor table not found: "+schema_from+"."+table_from)          
            return
        
        # SQL to retrieve data
        # TODO: Hardcoded
        sql = "SELECT LStepDate, LStepValue FROM "+schema_from+"."+table_from+" WHERE LStepDate > "+str(previous_date)+" ORDER BY LStepDate"
        rows = db_from.query_sql(sql)
        total = len(rows)
        self.logger.info("{job_copy_data} Sensor "+str(sensor_id)+" - Records found: "+str(total)) 
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
        # TODO: Hardcoded
        if total > 0:
            list_insert = []
            first_row = True
            table_to = "scada_"+str(self.scada_id)        
            sql_to = "INSERT INTO var."+table_to+" (sensor_id, step_date, step_value) VALUES ("
            for row in rows:
                values = str(sensor_id)+", "+xstr(row[0])+", "+xstr(row[1])
                aux = sql_to+values+")"
                list_insert.append(aux)
                if first_row:
                    first_date = str(row[0])
                    first_row = False
                last_date = xstr(row[0])
                                  
            query = ';\n'.join(list_insert)
            query = query+";\n"           
    
            # Insert process info into 'log_detail' table
            sql = "INSERT INTO audit.log_detail (scada_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
            log_detail = str(self.scada_id)+", "+str(sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total)+", '"+db_from.get_host()+"'"
            sql = sql+log_detail+");"
            query = query+sql
            self.db_to.execute_sql(query)                
            
            # Commit all changes in current thread
            self.db_to.commit()
        
        # Close connection to origin database for this thread 
        db_from.close()    
        
        self.logger.info("{job_copy_data} Sensor "+str(sensor_id)+" - End")          


    def copy_data(self, min_id=None, max_id=None, limit=None, delete_previous_data=False):

        self.logger.info("{copy_data} Start process")       
        
        if delete_previous_data:
            self.logger.info("{copy_data} Delete previous data")               
            self.delete_previous_data()
            
        # Create SQL to retrieve sensors
        sql = "SELECT id FROM var.sensor WHERE scada_id = "+str(self.scada_id)
        if min_id is not None:
            sql = sql+" AND id >= "+str(min_id)
        if max_id is not None:
            sql = sql+" AND id <= "+str(max_id)
        sql = sql+" ORDER BY id"
        if limit is not None:
            sql = sql+" LIMIT "+str(limit)
        rows = self.db_to.query_sql(sql)        
         
        # Iterate over all returned sensors         
        for row in rows:        
            sensor_id = row[0]
            self.create_sensor_job(sensor_id)          
               
        self.logger.info("{copy_data} Previous dates:\n"+str(self.sensor_dates))   
         
        # Run all jobs (one per sensor) with selected delayed seconds between each one             
        schedule.run_all(self.sleep)       


    def create_sensor_job(self, sensor_id):

        # Define sensor job
        # Get id of the last record inserted
        jobObj = schedule.every(self.interval).minutes
        jobObj.do(self.run_threaded, self.job_copy_data, sensor_id)
        previous_date = self.get_last_row(self.scada_id, sensor_id)
        self.sensor_dates[sensor_id] = previous_date      
        
            
    def delete_previous_data(self):
        
        sql = "DELETE FROM audit.log_detail WHERE scada_id = "+str(self.scada_id)
        self.db_to.execute_sql(sql)   
        sql = "DELETE FROM var.scada_"+str(self.scada_id)
        self.db_to.execute_sql(sql)           
        self.db_to.commit()        
        
        