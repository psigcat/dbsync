# -*- coding: utf-8 -*-
import pymssql


class MsSqlDao():

    def __init__(self):
        pass

    def get_host(self):
        return self.host
    
    def set_params(self, host, port, db, user, pwd):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pwd = pwd   
        
        
    def init_db(self):
        self.conn = pymssql.connect(self.host, self.user, self.pwd, self.db)
        self.cursor = self.conn.cursor()
        return True


    def test(self):
        self.cursor.execute('SELECT TOP 5 * FROM dbo.Line')
        row = self.cursor.fetchone()
        while row:
            #print("ID=%d, Name=%s" % (row[0], row[1]))
            print row[3]
            row = self.cursor.fetchone()
        self.conn.close()        
        
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
