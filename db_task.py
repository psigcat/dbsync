# -*- coding: utf-8 -*-
import time
import logging
import threading
from datetime import datetime

from dao.pg_dao import PgDao
from dao.mssql_dao import MsSqlDao

import lib.schedule as schedule
from utils import *  # @UnusedWildImport


class DbTask():
    

    def __init__(self):
        ''' Constructor class '''
        self.logger = logging.getLogger('dbsync') 
        self.sensor_dates = {} 
        self.sensor_dates_logical = {} 
        self.exists = False
        self.audit_table = "audit.log_detail"
        self.schema_from = "dbo"          
        self.schema_to = "var"
        self.table_scada = self.schema_to+".scada_master"
    
    
    def set_database_params(self, host, port, db, user, pwd, sgbd):
        ''' Set Database parameters '''
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pwd = pwd
        self.sgbd = sgbd
        
        
    def set_main_params(self, service_id, interval, sleep, default_start_tstamp, track_all_records):
        ''' Set Main parameters '''
        self.service_id = service_id
        self.interval = interval
        self.sleep = sleep
        self.default_start_tstamp = default_start_tstamp
        self.track_all_records = track_all_records
        

    def set_settings(self, settings):
        self.settings = settings


    def set_db_to(self, db):
        self.db_to = db
        
       
    def run_threaded(self, *args):
        job_thread = threading.Thread(target=args[0], args=(args[1],args[2]))
        job_thread.start()
        
        
    def create_thread_conn(self):        
        ''' Set origin Database. Connect to local Database (by default MSSQL) '''
        if self.sgbd.lower() == 'mssql':
            db_from = MsSqlDao()
        else:
            db_from = PgDao()          
        db_from.set_params(self.host, self.port, self.db, self.user, self.pwd)
        ok = db_from.init_db()  
        if ok:
            return db_from 


    def get_last_date(self, service, sensor, sensor_type=1):
        ''' Get last row inserted for the selected scada and sensor '''
        
        sql = "SELECT MAX(last_date) FROM "+self.audit_table
        sql = sql + " WHERE service_id = "+str(service)+" AND sensor_id = "+str(sensor)   
        if sensor_type <> 1:
            sql = sql + " AND sensor_type = "+str(sensor_type)
        result = None
        try:
            row = self.db_to.get_row(sql)
            result = row[0]
        except Exception as e:
            self.logger.warning('{get_last_date} Error %s' % e)
        finally:
            if result is None:
                result = self.default_start_tstamp
            return str(result)
                
        
              
    def job_copy_data_1(self, sensor_id, sensor_type=1):
        ''' Job task for scada model = 1 '''
        
        # Get date of the last record inserted
        previous_date = self.sensor_dates[sensor_id]
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database
        table_from = "LineStep_"+str(sensor_id)
        exists = db_from.check_table(self.schema_from, table_from)
        if not exists:
            self.logger.info("Sensor table not found: "+self.schema_from+"."+table_from)          
            return
        
        # SQL to retrieve data
        sql = "SELECT LStepDate, LStepValue FROM "+self.schema_from+"."+table_from+" WHERE LStepDate > "+str(previous_date)+" ORDER BY LStepDate"
        rows = db_from.query_sql(sql)
        total_found = len(rows)
        total_inserted = 0        
        self.logger.info("Sensor "+str(sensor_id)+" - Records found: "+str(total_found)) 
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
        if total_found > 0:
            i = 0
            previous_value = None            
            list_insert = []   
            table_to = "scada_"+str(self.service_id)        
            sql_to = "INSERT INTO "+self.schema_to+"."+table_to+" (sensor_id, step_date, step_value) VALUES ("
            for row in rows:
                # Set first and last date
                i = i + 1
                if i == 1:               
                    first_date = date_to_tstamp(xstr(row[0])[:-8])  
                if i == total_found:                 
                    last_date = date_to_tstamp(xstr(row[0])[:-8])
                # Check if value has changed or we want to track all records
                if previous_value <> row[1] or self.track_all_records == 1:
                    date_aux = xstr(row[0])                    
                    values = str(sensor_id)+", "+date_aux+", "+xstr(row[1])
                    aux = sql_to+values+")"
                    list_insert.append(aux)
                    previous_value = row[1]  
                    total_inserted = total_inserted + 1               
                                  
            self.job_post_process(list_insert, self.audit_table, sensor_id, first_date, last_date, total_found, total_inserted)
        
        self.job_close_connection(db_from, sensor_id, total_inserted) 
        
               
    def job_copy_data_2(self, sensor_id, sensor_type=1):
        ''' Job task for scada model = 2 '''                
        
        # Define table and column names depending its sensor_type     
        if sensor_type == 1:
            table_from = "ArchivedNumericInformations"   
            col_id = "numericInformation_id"   
            previous_date = self.sensor_dates[sensor_id]
        else:
            table_from = "ArchivedLogicalInformations"   
            col_id = "logicalInformation_id"              
            previous_date = self.sensor_dates_logical[sensor_id]
                                     
        # Format previous date (if is the default one provided)
        if previous_date == str(self.default_start_tstamp):
            date_object = datetime.strptime(previous_date, '%Y%m%d')   
            previous_date = date_object.strftime("%Y-%m-%d %H:%M:%S")       
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database (only first time)
        if not self.exists:
            self.exists = db_from.check_table(self.schema_from, table_from)    
            if not self.exists:                    
                self.logger.info("Sensor table not found: "+self.schema_from+"."+table_from)          
                return
        
        # SQL to retrieve data
        sql = "SELECT date, value FROM "+self.schema_from+"."+table_from+\
            " WHERE "+col_id+" = "+str(sensor_id)+" AND date > '"+str(previous_date)+"' AND date < '"+str(self.date_to)+"'"
        sql = sql + " ORDER BY date"                  
        rows = db_from.query_sql(sql)
        
        # Process returned rows
        self.job_process_rows(rows, sensor_id, sensor_type)
        
        # Close connection to origin database for this thread 
        db_from.close()            
                         
               
    def job_copy_data_3(self, sensor_id, sensor_type=1):
        ''' Job task for scada model = 3 '''          
                  
        # Get table and column names from sensor table 
        sql = "SELECT table_name, column_name FROM "+self.schema_to+".sensor_"+str(self.service_id)+" WHERE id = "+str(sensor_id)
        row = self.db_to.get_row(sql)
        table_from = row[0]
        col_from = row[1]  
        previous_date = self.sensor_dates[sensor_id]
                                     
        # Format previous date (if is the default one provided)
        if previous_date == str(self.default_start_tstamp):
            date_object = datetime.strptime(previous_date, '%Y%m%d')   
            previous_date = date_object.strftime("%Y-%m-%d %H:%M:%S")      
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database (only first time)
        if not self.exists:
            self.exists = db_from.check_table(self.schema_from, table_from)    
            if not self.exists:                    
                self.logger.info("Sensor table not found: "+self.schema_from+"."+table_from)          
                return     
        
        # SQL to retrieve data
        sql = "SELECT Time_Stamp, "+col_from+" FROM "+self.schema_from+"."+table_from+\
            " WHERE Time_Stamp > '"+str(previous_date)+"' AND Time_Stamp < '"+str(self.date_to)+"'"
        sql = sql + " ORDER BY Time_Stamp"  
        #self.logger.info("{job_copy_data_3}\n"+str(sql))                          
        rows = db_from.query_sql(sql)

        # Process returned rows
        self.job_process_rows(rows, sensor_id)
        
        # Close connection to origin database for this thread 
        db_from.close()       


    def job_process_rows(self, rows, sensor_id, sensor_type=1):
        ''' Process returned rows '''
        
        total_found = len(rows)
        total_inserted = 0        
        self.logger.info("Sensor "+str(sensor_id)+" ("+str(sensor_type)+") - Records found: "+str(total_found))  
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
        if total_found > 0:        
            i = 0
            total_inserted = 0        
            previous_value = None            
            list_insert = []   
            sql_to = "INSERT INTO "+self.table_scada+" (service_id, sensor_id, step_date, step_value, sensor_type) VALUES ("
            for row in rows:
                # Manage first and last date
                i = i + 1
                if i == 1:               
                    first_date = xstr(row[0])[:-8]  
                if i == total_found:                 
                    last_date = xstr(row[0])[:-8]
                # Check if value has changed or we want to track all records
                if previous_value <> row[1] or self.track_all_records == 1:               
                    date_aux = xstr(row[0])[:-8]   
                    if sensor_type == 1:                                   
                        values = str(self.service_id)+", "+str(sensor_id)+", '"+date_aux+"', "+xstr(row[1])+", "+str(sensor_type)                                      
                    else:
                        values = str(self.service_id)+", "+str(sensor_id)+", '"+date_aux+"', "+xstr(int(row[1]))+", "+str(sensor_type)
                    aux = sql_to+values+")"
                    list_insert.append(aux)
                    previous_value = row[1]  
                    total_inserted = total_inserted + 1
                                           
            self.job_post_process(list_insert, self.audit_table, sensor_id, sensor_type, first_date, last_date, total_found, total_inserted)    
            
            
    def job_post_process(self, list_insert, audit_table, sensor_id, sensor_type, first_date, last_date, total_found, total_inserted):
        ''' Final step of the task. Insert into 'log_detail table and commit changes '''
        
        # Complete query with inserted records
        query = ';\n'.join(list_insert)
        query = query+";\n"   
            
        # Insert process info into 'log_detail' table
        sql = "INSERT INTO "+audit_table+" (service_id, sensor_id, sensor_type, first_date, last_date, rec_found, rec_inserted) VALUES ("
        log_detail = str(self.service_id)+", "+str(sensor_id)+", "+str(sensor_type)+", '"+first_date+"', '"+last_date+"', "+str(total_found)+", "+str(total_inserted)
        sql = sql+log_detail+");"
        query = query+sql
        self.logger.info("Sensor "+str(sensor_id)+" ("+str(sensor_type)+") - Records inserted: "+str(total_inserted))        
        self.logger.info(sql)  
        self.db_to.execute_sql(query)  
        
        # Commit all changes in current thread
        self.db_to.commit()  
                

    def copy_data(self, min_id=-1, max_id=-1, limit=-1, delete_previous_data=False):
        ''' Copy data from origin to destination Database '''
        
        self.logger.info("{copy_data} Start process")       
        
        if delete_previous_data:
            self.logger.info("{copy_data} Delete previous data")               
            self.delete_previous_data()
            
        # Get parameter 'date_to'
        self.date_to = self.settings.get('main', 'date_to')
        date_object = datetime.strptime(self.date_to, '%Y%m%d')   
        self.date_to = date_object.strftime("%Y-%m-%d %H:%M:%S")   
        self.logger.info("{copy_data} Parameter 'date_to': "+str(self.date_to))   
         
        # Get scada model from selected service_id
        sql = "SELECT model_id FROM var.service WHERE id = "+str(self.service_id)
        row = self.db_to.get_row(sql)
        model = row[0]
        
        # Set SQL and job function depending of its scada model   
        if model == 1:
            job_function = self.job_copy_data_1
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+" WHERE status_id = 'active'"
        elif model == 2:
            job_function = self.job_copy_data_2
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+" WHERE status_id = 'active'"
        elif model == 3:
            job_function = self.job_copy_data_3
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+" WHERE status_id = 'active'"
            
        # Create SQL to retrieve sensors
        if min_id != -1:
            sql = sql+" AND id >= "+str(min_id)
        if max_id != -1:
            sql = sql+" AND id <= "+str(max_id)
        sql = sql+" ORDER BY id"
        if limit != -1:
            sql = sql+" LIMIT "+str(limit)
        self.logger.info("{copy_data} "+str(sql))              
        
        # Check if we have to process numeric sensors (sensor_type = 1)
        process_numeric_sensors = self.settings.get('main', 'process_numeric_sensors')
        process_numeric_sensors = int(process_numeric_sensors)
        if process_numeric_sensors == 1:        
            # Iterate over all returned sensors         
            rows = self.db_to.query_sql(sql)        
            for row in rows:       
                # Define sensor job
                sensor_id = row[0]
                self.create_sensor_job(job_function, sensor_id, 1) 
                # Get id of the last record inserted
                previous_date = self.get_last_date(self.service_id, sensor_id, 1)
                self.sensor_dates[sensor_id] = previous_date      
                    
            self.logger.info("{copy_data} Previous dates (numeric):\n"+str(self.sensor_dates))   
        
        
        # Check if we have to process logical sensors (sensor_type = 2)
        process_logical_sensors = self.settings.get('main', 'process_logical_sensors')
        process_logical_sensors = int(process_logical_sensors)
        if model == 2 and process_logical_sensors == 1:
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+"_logical WHERE status_id = 'active'"
            # Create SQL to retrieve sensors
            if min_id != -1:
                sql = sql+" AND id >= "+str(min_id)
            if max_id != -1:
                sql = sql+" AND id <= "+str(max_id)
            sql = sql+" ORDER BY id"
            if limit != -1:
                sql = sql+" LIMIT "+str(limit)
            self.logger.info("{copy_data} "+str(sql))              
            rows = self.db_to.query_sql(sql)        
            
            # Iterate over all returned sensors         
            for row in rows:       
                # Define sensor job
                sensor_id = row[0]
                self.create_sensor_job(job_function, sensor_id, 2) 
                # Get id of the last record inserted
                previous_date = self.get_last_date(self.service_id, sensor_id, 2)
                self.sensor_dates_logical[sensor_id] = previous_date      
                   
            self.logger.info("{copy_data} Previous dates (logical):\n"+str(self.sensor_dates_logical))   
         
        # Run all jobs (one per sensor) with selected delayed seconds between each one             
        schedule.run_all(self.sleep)       


    def create_sensor_job(self, job_function, sensor_id, sensor_type=1):
        ''' Set a new job for selected sensor '''
        jobObj = schedule.every(self.interval).minutes
        jobObj.do(self.run_threaded, job_function, sensor_id, sensor_type)

            
    def delete_previous_data(self):
        ''' Delete previous data from audit and scada table from selected service '''
        sql = "DELETE FROM "+self.audit_table+" WHERE service_id = "+str(self.service_id)
        self.db_to.execute_sql(sql)   
        sql = "DELETE FROM "+self.table_scada+" WHERE service_id = "+str(self.service_id)
        self.db_to.execute_sql(sql)           
        self.db_to.commit()        
        
        