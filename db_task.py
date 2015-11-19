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
        
    def set_sensor_id(self, sensor_id):
        self.sensor_id = sensor_id

    def set_db_from(self, db):
        self.db_from = db

    def set_db_to(self, db):
        self.db_to = db

    def run_threaded(self, job_func):
        job_thread = threading.Thread(target=job_func)
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
        
        
    def job_copy_data(self):

        aux = time.strftime('%x %X')
        print str(aux)
        print("I'm running on thread %s" % threading.current_thread())

        # Get id of the last record inserted
        # TODO: hardcoded to sensor = 5
        self.sensor_id = 5
        previous_data = self.get_last_row(self.scada_id, self.sensor_id)
        
        # Retrieve data
        first_row = True
        table_from = "dbo.LineStep_"+str(self.sensor_id)
        sql = "SELECT TOP 2 LStepDate, LStepValue FROM "+table_from+" WHERE LStepDate > "+str(previous_data)+" ORDER BY LStepDate"
        #sql = sql + " LIMIT 2"
        
        # SQL instruction to Insert data into destination table
        table_to = "scada_"+str(self.scada_id)        
        sql_to = "INSERT INTO var."+table_to+" (sensor_id, step_date, step_value) VALUES ("
        list_insert = []
        rows = self.db_from.query_sql(sql)
        total = str(len(rows))
        for row in rows:
            values = str(self.sensor_id)+", "+xstr(row[0])+", "+xstr(row[1])
            aux = sql_to+values+")"
            list_insert.append(aux)
            if first_row:
                first_date = str(row[0])
                first_row = False
            last_date = xstr(row[0])

        for elem in list_insert:
            self.db_to.execute_sql(elem)

        # Insert info into 'log_detail' table
        #query = ';\n'.join(list_insert)
        sql = "INSERT INTO var.log_detail (scada_id, sensor_id, first_date, last_date, rec_number, addr) VALUES ("
        log_detail = str(self.scada_id)+", "+str(self.sensor_id)+", "+str(first_date)+", "+str(last_date)+", "+str(total)+", '"+self.db_from.get_host()+"'"
        sql = sql+log_detail+")"
        self.db_to.execute_sql(sql)        
        
        # Commit all changes
        self.db_to.commit()


    def copy_data(self):

        jobObj = schedule.every(4).seconds
        jobObj.do(self.run_threaded, self.job_copy_data)
        #jobObj.do(self.job_copy_data)
        i = 0
        while i < 15:
            i = i + 1
            schedule.run_pending()
            time.sleep(1)


    def job_test(self):
        aux = time.strftime('%x %X')
        print str(aux)
        print("I'm running on thread %s" % threading.current_thread())


    def test(self):

        jobObj = schedule.every(5).seconds
        jobObj.do(self.run_threaded, self.job_test)
        i = 0
        while i < 15:
            i = i + 1
            schedule.run_pending()
            time.sleep(1)
            
            