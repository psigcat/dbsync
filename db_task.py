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
        self.logger = logging.getLogger('dbsync') 
        self.sensor_dates = {} 
        self.sensor_dates_logical = {} 
        self.exists = False
        self.audit_table = "log_detail"
    
    def set_database_params(self, host, port, db, user, pwd, sgbd):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pwd = pwd
        self.sgbd = sgbd
        
    def set_main_params(self, service_id, interval, sleep, default_start_tstamp, time_gap, track_all_records):
        self.service_id = service_id
        self.interval = interval
        self.sleep = sleep
        self.default_start_tstamp = default_start_tstamp
        self.time_gap = time_gap
        self.track_all_records = track_all_records

    def set_db_to(self, db):
        self.db_to = db
       
    def run_threaded(self, *args):
        job_thread = threading.Thread(target=args[0], args=(args[1],args[2]))
        job_thread.start()
        
        
    def create_thread_conn(self):        
               
        # DB origin. Connect to local Database (by default MSSQL)
        if self.sgbd.lower() == 'mssql':
            db_from = MsSqlDao()
        else:
            db_from = PgDao()          
        db_from.set_params(self.host, self.port, self.db, self.user, self.pwd)
        ok = db_from.init_db()  
        if ok:
            return db_from 


    # Get last row inserted for the selected scada and sensor
    def get_last_row(self, service, sensor, audit_table="log_detail"):
        
        sql = "SELECT MAX(last_date) FROM audit."+audit_table
        sql = sql + " WHERE service_id = "+str(service)+" AND sensor_id = "+str(sensor)   
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
                
        
    # Job task for scada model = 1            
    def job_copy_data_1(self, sensor_id, sensor_type="numeric"):

        self.logger.info("Sensor "+str(sensor_id)+" - "+str(threading.current_thread()))
 
        # Get id of the last record inserted
        previous_date = self.sensor_dates[sensor_id]
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database
        schema_from = "dbo"
        table_from = "LineStep_"+str(sensor_id)
        exists = db_from.check_table(schema_from, table_from)
        if not exists:
            self.logger.info("Sensor table not found: "+schema_from+"."+table_from)          
            return
        
        # SQL to retrieve data
        sql = "SELECT LStepDate, LStepValue FROM "+schema_from+"."+table_from+" WHERE LStepDate > "+str(previous_date)+" ORDER BY LStepDate"
        rows = db_from.query_sql(sql)
        total = len(rows)
        self.logger.info("Sensor "+str(sensor_id)+" - Records found: "+str(total)) 
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
        if total > 0:
            list_insert = []
            first_row = True
            table_to = "scada_"+str(self.service_id)        
            sql_to = "INSERT INTO var."+table_to+" (sensor_id, step_date, step_value) VALUES ("
            for row in rows:
                values = str(sensor_id)+", "+xstr(row[0])+", "+xstr(row[1])
                aux = sql_to+values+")"
                list_insert.append(aux)
                if first_row:
                    first_date = xstr(row[0])
                    first_row = False
                last_date = xstr(row[0])
                                  
            query = ';\n'.join(list_insert)
            query = query+";\n"           
    
            # Insert process info into 'log_detail' table
            sql = "INSERT INTO audit.log_detail (service_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
            log_detail = str(self.service_id)+", "+str(sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total)+", '"+db_from.get_host()+"'"
            sql = sql+log_detail+");"
            query = query+sql
            self.db_to.execute_sql(query)           
            
            # Commit all changes in current thread
            self.db_to.commit()
        
        # Close connection to origin database for this thread 
        db_from.close()    
        
        self.logger.info("Sensor "+str(sensor_id)+" - Records inserted: "+str(total))      
        
               
    # Job task for scada model = 2                
    def job_copy_data_2(self, sensor_id, sensor_type="numeric"):

        self.logger.info("Sensor "+str(sensor_id)+" - "+str(threading.current_thread()))
        
        # Define schema, table and column names depending its sensor_type
        schema_from = "dbo"
        schema_to = "var"             
        if sensor_type == 'numeric':
            audit_table = self.audit_table
            table_from = "ArchivedNumericInformations"
            table_to = "scada_"+str(self.service_id)     
            col_id = "numericInformation_id"   
            previous_date = self.sensor_dates[sensor_id]
        else:
            audit_table = self.audit_table+"_logical"
            table_from = "ArchivedLogicalInformations"  
            table_to = "scada_"+str(self.service_id)+"_logical"    
            col_id = "[logicalInformation_id]"              
            previous_date = self.sensor_dates_logical[sensor_id]
            self.logger.info("Sensor type: "+sensor_type)
                                     
        # Format previous date
        date_object = datetime.strptime(previous_date, '%Y%m%d%H%M%S')   
        previous_date = date_object.strftime("%Y-%m-%d %H:%M:%S")      
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database (only first time)
        if not self.exists:
            self.exists = db_from.check_table(schema_from, table_from)    
            if not self.exists:                    
                self.logger.info("Sensor table not found: "+schema_from+"."+table_from)          
                return
        
        # SQL to retrieve data
        sql = "SELECT date, value FROM "+schema_from+"."+table_from+\
            " WHERE "+col_id+" = "+str(sensor_id)+" AND date > '"+str(previous_date)+"'"
        if sensor_type == 'numeric' and self.time_gap != -1:
            sql = sql + " AND DATEPART(hour, date) % "+str(self.time_gap)+" = 0 AND DATEPART(minute, date) = '00'" 
        sql = sql + " ORDER BY date"                  
        rows = db_from.query_sql(sql)
        total = len(rows)
        self.logger.info("Sensor "+str(sensor_id)+" - Records found: "+str(total)) 
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
        if total > 0:
            list_insert = []
            first_row = True   
            sql_to = "INSERT INTO "+schema_to+"."+table_to+" (sensor_id, step_date, step_value) VALUES ("
            for row in rows:
                # Format date
                date_aux = xstr(row[0])[:-8]
                date_object = datetime.strptime(date_aux, "%Y-%m-%d %H:%M:%S")
                date_aux = date_object.strftime("%Y%m%d%H%M%S") 
                # TODO: int(row[1]) only valid fur logical!                   
                values = str(sensor_id)+", "+date_aux+", "+xstr(int(row[1]))
                aux = sql_to+values+")"
                list_insert.append(aux)
                if first_row:
                    first_date = date_aux
                    first_row = False
                last_date = date_aux
                                  
            query = ';\n'.join(list_insert)
            query = query+";\n"                   
            
            # Insert process info into 'log_detail' table
            sql = "INSERT INTO audit."+audit_table+" (service_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
            log_detail = str(self.service_id)+", "+str(sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total)+", '"+db_from.get_host()+"'"
            sql = sql+log_detail+");"
            query = query+sql
            self.db_to.execute_sql(query)                
            
            # Commit all changes in current thread
            self.db_to.commit()
        
        # Close connection to origin database for this thread 
        db_from.close()    
        
        self.logger.info("Sensor "+str(sensor_id)+" - Records inserted: "+str(total))          
               
               
    # Job task for scada model = 3          
    def job_copy_data_3(self, sensor_id, sensor_type="numeric"):

        self.logger.info("Sensor "+str(sensor_id)+" - "+str(threading.current_thread()))
        
        schema_from = "dbo"
        schema_to = "var"             
        table_to = "scada_"+str(self.service_id)     
        audit_table = self.audit_table
        
        # Get table and column names from sensor table
        sql = "SELECT table_name, column_name FROM "+schema_to+".sensor_"+str(self.service_id)+" WHERE id = "+str(sensor_id)
        row = self.db_to.get_row(sql)
        table_from = row[0]
        col_from = row[1]  
        previous_date = self.sensor_dates[sensor_id]
                                     
        # Format previous date
        date_object = datetime.strptime(previous_date, '%Y%m%d%H%M%S')   
        previous_date = date_object.strftime("%Y-%m-%d %H:%M:%S")      
        
        # Create a new connection to origin database for this thread
        db_from = self.create_thread_conn()
        if db_from is None:
            self.logger.info("Sensor "+str(sensor_id)+" - Error connecting to DB "+str(threading.current_thread()))              
            return

        # Check if the sensor table exists in the origin Database (only first time)
        if not self.exists:
            self.exists = db_from.check_table(schema_from, table_from)    
            if not self.exists:                    
                self.logger.info("Sensor table not found: "+schema_from+"."+table_from)          
                return
        
        # SQL to retrieve data
        sql = "SELECT Time_Stamp, "+col_from+" FROM "+schema_from+"."+table_from+\
            " WHERE Time_Stamp > '"+str(previous_date)+"'"
        sql = sql + " ORDER BY Time_Stamp"                  
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
            sql_to = "INSERT INTO "+schema_to+"."+table_to+" (sensor_id, step_date, step_value) VALUES ("
            for row in rows:
                # Set first and last date
                i = i + 1
                if i == 1:               
                    first_date = date_to_tstamp(xstr(row[0])[:-8])  
                if i == total_found:                 
                    last_date = date_to_tstamp(xstr(row[0])[:-8])
                # Check if value has changed or we want to track all records
                if previous_value <> row[1] or self.track_all_records == 1:
                    date_aux = date_to_tstamp(xstr(row[0])[:-8])                    
                    values = str(sensor_id)+", "+date_aux+", "+xstr(row[1])
                    aux = sql_to+values+")"
                    list_insert.append(aux)
                    previous_value = row[1]  
                    total_inserted = total_inserted + 1
                                  
            query = ';\n'.join(list_insert)
            query = query+";\n"                   
            
            # Insert process info into 'log_detail' table
            sql = "INSERT INTO audit."+audit_table+" (service_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
            log_detail = str(self.service_id)+", "+str(sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total_inserted)+", '"+db_from.get_host()+"'"
            sql = sql+log_detail+");"
            query = query+sql
            self.db_to.execute_sql(query)                
            
            # Commit all changes in current thread
            self.db_to.commit()
        
        # Close connection to origin database for this thread 
        db_from.close()    
        
        self.logger.info("Sensor "+str(sensor_id)+" - Records inserted: "+str(total_inserted))          


    def copy_data(self, min_id=None, max_id=None, limit=None, delete_previous_data=False):

        self.logger.info("{copy_data} Start process")       
        
        if delete_previous_data:
            self.logger.info("{copy_data} Delete previous data")               
            self.delete_previous_data()
         
        # Get scada model from selected service_id
        sql = "SELECT model_id FROM var.service WHERE id = "+str(self.service_id)
        row = self.db_to.get_row(sql)
        model = row[0]
        
        # Set SQL and job function depending of its scada model   
        if model == 1:
            job_function = self.job_copy_data_1
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)
            sql_logical = None  
        elif model == 2:
            job_function = self.job_copy_data_2
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+" WHERE status_id = 'active'"
            sql_logical = "SELECT id FROM var.sensor_"+str(self.service_id)+"_logical WHERE status_id = 'active'"
        elif model == 3:
            job_function = self.job_copy_data_3
            sql = "SELECT id FROM var.sensor_"+str(self.service_id)+" WHERE status_id = 'active'"
            
        # Create SQL to retrieve sensors
        if min_id is not None:
            sql = sql+" AND id >= "+str(min_id)
        if max_id is not None:
            sql = sql+" AND id <= "+str(max_id)
        sql = sql+" ORDER BY id"
        if limit is not None:
            sql = sql+" LIMIT "+str(limit)
        self.logger.info("{copy_data} "+str(sql))              
        rows = self.db_to.query_sql(sql)        
        
        # Iterate over all returned sensors         
        for row in rows:       
            # Define sensor job
            sensor_id = row[0]
            self.create_sensor_job(job_function, sensor_id) 
            # Get id of the last record inserted
            previous_date = self.get_last_row(self.service_id, sensor_id)
            self.sensor_dates[sensor_id] = previous_date      
               
        self.logger.info("{copy_data} Previous dates:\n"+str(self.sensor_dates))   
        
        # Process sensors of type logical (if any)
        if model == 2:
            sql = sql_logical
            # Create SQL to retrieve sensors
            if min_id is not None:
                sql = sql+" AND id >= "+str(min_id)
            if max_id is not None:
                sql = sql+" AND id <= "+str(max_id)
            sql = sql+" ORDER BY id"
            if limit is not None:
                sql = sql+" LIMIT "+str(limit)
            self.logger.info("{copy_data} "+str(sql))              
            rows = self.db_to.query_sql(sql)        
            
            # Iterate over all returned sensors         
            for row in rows:       
                # Define sensor job
                sensor_id = row[0]
                self.create_sensor_job(job_function, sensor_id, "logical") 
                # Get id of the last record inserted
                previous_date = self.get_last_row(self.service_id, sensor_id, "log_detail_logical")
                self.sensor_dates_logical[sensor_id] = previous_date      
                   
            self.logger.info("{copy_data} Previous dates (logical):\n"+str(self.sensor_dates_logical))   
         
        # Run all jobs (one per sensor) with selected delayed seconds between each one             
        schedule.run_all(self.sleep)       


    def create_sensor_job(self, job_function, sensor_id, sensor_type="numeric"):
        
        jobObj = schedule.every(self.interval).minutes
        jobObj.do(self.run_threaded, job_function, sensor_id, sensor_type)

            
    def delete_previous_data(self):
        
        sql = "DELETE FROM audit.log_detail WHERE service_id = "+str(self.service_id)
        self.db_to.execute_sql(sql)   
        sql = "DELETE FROM var.scada_"+str(self.service_id)
        self.db_to.execute_sql(sql)           
        self.db_to.commit()        
        
        