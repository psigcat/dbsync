import sys
if 'utils' in sys.modules:
    del sys.modules["utils"]
if 'threading' in sys.modules:
    del sys.modules["threading"]
from utils import xstr
import threading
import time
import schedule


class DbTask():

    def set_scada_id(self, scada_id):
        self.scada_id = scada_id
        
    def set_interval(self, interval):
        self.interval = interval
        
    def set_sleep(self, sleep):
        self.sleep = sleep

    def set_db_from(self, db):
        self.db_from = db

    def set_db_to(self, db):
        self.db_to = db
       
    def run_threaded(self, *args):
        job_thread = threading.Thread(target=args[0], args=(args[1],))
        job_thread.start()


    # Get last row inserted for the selected scada and sensor
    def get_last_row(self, scada, sensor):
        
        sql = "SELECT MAX(last_date) FROM var.log_detail"
        sql = sql + " WHERE scada_id = "+str(scada)+" AND sensor_id = "+str(sensor)
        row = self.db_to.get_row(sql)
        result = row[0]
        if result is None:
            result = -1
        return str(result)
                
        
    def job_copy_data(self, sensor_id):

        print "\n"+str(time.strftime('%x %X'))+" [job_copy_data] Start  Sensor: "+str(sensor_id)+" "+str(threading.current_thread())
 
        # Get id of the last record inserted
        previous_data = self.get_last_row(self.scada_id, sensor_id)
        
        # SQL to retrieve data
        table_from = "dbo.LineStep_"+str(sensor_id)
        sql = "SELECT LStepDate, LStepValue FROM "+table_from+" WHERE LStepDate > "+str(previous_data)+" ORDER BY LStepDate"
        rows = self.db_from.query_sql(sql)
        total = len(rows)
        print "\n"+str(time.strftime('%x %X'))+" [job_copy_data] Records found: "+str(total)  
            
        # If we have found at least one record
        # Define SQL's to Insert data into destination table
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
            self.db_to.execute_sql(query)               
    
            # Insert process info into 'log_detail' table
            sql = "INSERT INTO var.log_detail (scada_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
            log_detail = str(self.scada_id)+", "+str(sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total)+", '"+self.db_from.get_host()+"'"
            sql = sql+log_detail+")"
            self.db_to.execute_sql(sql)                
            
            # Commit all changes
            self.db_to.commit()
            
        print "\n"+str(time.strftime('%x %X'))+" [job_copy_data] End    Sensor: "+str(sensor_id)           


    def copy_data(self, loop=False):

        print "\n"+str(time.strftime('%x %X'))+" [copy_data] Start process"      
        
        # Iterate over all sensors
        sql = "SELECT id FROM var.sensor WHERE scada_id = "+str(self.scada_id)+" ORDER BY id"
        #sql = sql+" LIMIT "+str(10)
        rows = self.db_to.query_sql(sql)
        for row in rows:        
            #print str(time.strftime('%x %X'))+" - Setting thread to manage sensor "+str(row[0])
            jobObj = schedule.every(self.interval).minutes
            jobObj.do(self.run_threaded, self.job_copy_data, row[0])
            if loop:
                time.sleep(self.sleep)               
        
        if loop:
            i = 0
            while i < self.interval * 60 * 3.5:
                i = i + 1
                schedule.run_pending()
                time.sleep(1)                  
        
        else:
            schedule.run_all(self.sleep)
            
        print str(time.strftime('%x %X'))+" [copy_data] End process\n"              


    def copy_data_sensor(self, sensor_id):

        # Define a thread to manage this sensor
        jobObj = schedule.every(self.interval).minutes
        jobObj.do(self.run_threaded, self.job_copy_data, sensor_id)
        schedule.run_all(self.sleep)
        print str(time.strftime('%x %X'))+" [copy_data_sensor] End process\n"           
        
            
    def truncate_log_detail(self):
        
        sql = "DELETE FROM var.log_detail"
        self.db_to.execute_sql(sql)   
        sql = "DELETE FROM var.scada_1"
        self.db_to.execute_sql(sql)           
        self.db_to.commit()        