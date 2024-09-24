import json
import logging
from datetime import datetime
import requests
from docebo_helper import os
import singlestoredb as s2
import time

class singlestore:
    def __init__(self, s2_config):
        self.host = s2_config.host
        self.port = s2_config.port
        self.database = s2_config.database
        self.user = s2_config.user
        self.password = s2_config.password
        
    def run_sql(self, statement):
        retry_count = 0
        try:
            if statement != "":
                conn = s2.connect(host = self.host, port = self.port, database = self.database, user = self.user, password = self.password)
                with conn:
                    with conn.cursor() as cursor:
                        cursor.execute(statement)
                        result = cursor.rowcount
                        return result
        except s2.clients.pymysqlsv.err.OperationalError as ex:
            if retry_count < 10:
                logging.info("retrying {0}".format(statement))
                time.sleep(2)
                self.run_sql(self,statement)
                retry_count +=1
                
        except Exception as ex:
            logging.exception(ex)

    def run_select(self, query):
        try:
            if query != "":
                conn = s2.connect(host = self.host, port = self.port, database = self.database, user = self.user, password = self.password)
                with conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        results = cursor.fetchall()
            return results
        except Exception as ex:
            logging.exception(ex)

    def delete_records(self, table_name):
        statement_text = "delete from {0};".format(table_name)
        self.run_sql(statement_text)
    
    def get_number_of_records(self, table_name):
            query_text = "select count(*) from {0};".format(table_name)
            result = self.run_select(query_text)
            rows = result[0]
            value = rows[0]
            return value
        
    def get_number_of_course_records(self, user_id, course_code):
        query_text = "select count(*) from enrollment where user_id = '{0}' and code = '{1}';".format(user_id, 
                                                                                                        course_code)
        result = []
        result = self.run_select(query_text)
        return result


#    def record_exists(self, id_in, table_name):
#        id_in = "{0}".format(id_in)
#        query_text = "select id from {0} where id = {1}".format(table_name, id_in)
#        ids = self.run_select(query_text)
#        if len(ids) == 0:
#                return False
#        else:
#                return True

    def get_number_of_records_for_user(self, username, course_code):
        query_text = "select count(*) from enrollment_temp where userName = '{0}' and courseCode = '{1}';".format(username, course_code)
        result = self.run_select( query_text)
        return result

    def copy_table(self, src_table, dest_table):
        statement_text = "insert into {0} * {1};"
        self.run_sql(statement_text)

class s2_config:
    def __init__(self, os_in) -> None:
        if os_in == os.windows:
            config_file = open(".\\config.json", "r")
        elif os_in == os.linux:
            config_file = open("./config.json", "r")
        elif os_in == os.mac:
            config_file = open("config.json", "r")
        config_text = config_file.read()
        config_file.close()
        config_json = json.loads(config_text)
        self.host = config_json["host"]
        self.port = config_json["port"]
        self.user = config_json["user"]
        self.password = config_json["password"]
        self.database = config_json["database"]