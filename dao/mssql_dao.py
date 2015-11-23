# -*- coding: utf-8 -*-
import logging
import lib.pymssql as pymssql


class MsSqlDao():

    def __init__(self):
        self.logger = logging.getLogger('dbsync')  

    def get_host(self):
        return self.host
    
    def set_params(self, host, port, db, user, pwd):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pwd = pwd   
        
    def init_db(self):
        try:
            self.conn = pymssql.connect(self.host, self.user, self.pwd, self.db)
            self.cursor = self.conn.cursor()
        except pymssql.OperationalError, e:
            self.logger.warning('Error %s' % e)
            return False
        return True              

    def query_sql(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return rows
    
    def get_row(self, sql):
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        return row

    def execute_sql(self, sql):
        self.cursor.execute(sql)      
        
    def check_table(self, schema, table): 
        sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+schema+"' AND TABLE_NAME = '"+table+"'"      
        row = self.get_row(sql)  
        exists = row is not None
        return exists
