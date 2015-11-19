# -*- coding: utf-8 -*-
from PyQt4.QtCore import * #@UnusedWildImport
from PyQt4.QtGui import * #@UnusedWildImport
from PyQt4.QtSql import * #@UnusedWildImport

import psycopg2
import psycopg2.extras


class PgDao():

    def __init__(self):
        pass

    def get_host(self):
        return self.host
    
    def init_db(self):
        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            status = True
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            status = False
        return status

    def set_params(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn_string = "host="+self.host+" port="+self.port+" dbname="+self.dbname+" user="+self.user+" password="+self.password

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

    def commit(self):
        self.conn.commit()

