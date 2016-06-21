# -*- coding: utf-8 -*-
import psycopg2.extras
from parent_dao import ParentDao


class PgDao(ParentDao):

    
    def init_db(self):
        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            status = True
        except psycopg2.DatabaseError, e:
            self.logger.warning('{pg_dao} Error %s' % e)
            status = False
        return status
    
    def checkTable(self, schemaName, tableName):
        exists = True
        sql = "SELECT * FROM pg_tables WHERE schemaname = '"+schemaName+"' AND tablename = '"+tableName+"'"    
        self.cursor.execute(sql)         
        if self.cursor.rowcount == 0:      
            exists = False
        return exists             

