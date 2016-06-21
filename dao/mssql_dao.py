# -*- coding: utf-8 -*-
import pymssql
from parent_dao import ParentDao


class MsSqlDao(ParentDao):


    def init_db(self):
        try:
            self.conn = pymssql.connect(self.host, self.user, self.password, self.dbname)
            self.cursor = self.conn.cursor()
        except pymssql.OperationalError, e:
            self.logger.warning('{mssql_dao} Error %s' % e)
            return False
        except pymssql.InterfaceError, e:
            self.logger.warning('{mssql_dao} Error %s' % e)
            return False
        return True              
        
    def check_table(self, schema, table): 
        sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+schema+"' AND TABLE_NAME = '"+table+"'"      
        row = self.get_row(sql)  
        exists = row is not None
        return exists

