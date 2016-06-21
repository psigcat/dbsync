# -*- coding: utf-8 -*-
import logging


class ParentDao():
        
    def __init__(self):
        self.logger = logging.getLogger('dbsync')  
        self.last_error = None        

    def get_host(self):
        return self.host
    
    def get_last_error(self):
        return self.last_error        

    def set_params(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn_string = "host="+self.host+" port="+self.port+" dbname="+self.dbname+" user="+self.user+" password="+self.password

    def get_rows(self, sql):
        rows = None
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()     
        except Exception as e:
            self.logger.warning(str(e)+sql)       
            self.rollback()    
        finally:                     
            return rows
    
    def get_row(self, sql):
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            self.logger.warning(str(e)+sql)       
            self.rollback()             
        finally:
            return row     

    def execute_sql(self, sql):
        self.last_error = None 
        status = True
        try:
            self.cursor.execute(sql) 
        except Exception as e:
            self.logger.warning(str(e)+sql)  
            self.last_error = e        
            status = False
            self.rollback() 
        finally:
            return status

    def commit(self):
        self.conn.commit()
        
    def close(self):
        self.conn.close()        
        
