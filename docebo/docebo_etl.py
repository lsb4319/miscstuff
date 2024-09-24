from datetime import datetime
from singlestore import singlestore, s2_config
from docebo_helper import docebo_helper
import logging

class docebo_etl:
    def __init__(self):
        pass
    
    @staticmethod
    def group_courses(s2conf):
        try:
            start_time = datetime.now()
            logging.info("group courses started: {0}".format(start_time))
            query_text = "select distinct code from enrollment where code like '%CUST-SP-%';"
            distinct_codes = []
            s2 = singlestore(s2conf)
            distinct_codes = s2.run_select(query_text)
            leave_list = ["X-CUST-SP-DEV-105"]
            rows_updated = 0
            for code in distinct_codes:
                str_code = code[0]
                if str_code.startswith("X-"):
                    for x in leave_list:
                        if str_code == x:
                                break
                        else:
                            update_code = docebo_etl.prep_course_code(str_code)
                            update_text = "update enrollment set code = '{0}' where code = '{1}';"\
                                .format(update_code, str_code)
                            result = s2.run_sql(update_text)
                            rows_updated += result                  
            end_time = datetime.now()
            elapsed_time = end_time-start_time
            logging.info("group courses finished: {0}".format(end_time))
            logging.info("group courses elapsed time: {0}".format(elapsed_time))
            logging.info("{0} rows updated".format(rows_updated))
        except Exception as ex:
                logging.exception(ex)

    @staticmethod            
    def remove_false_enrollments(s2conf):
        try:
            start_time = datetime.now()
            logging.info("remove false enrollments started: {0}".format(start_time))
            s2 = singlestore(s2conf)
            query_text = "delete from enrollment e \
                where subscribed_by in (21828,21704,17104,17065,20042,13089,15489,13059) \
                and subscribed_by <> user_id \
                and code like 'CUST-SP-%' \
                AND total_time = 0 \
                and status = 'enrolled' \
                and type <> 'learning_plan' \
                ;"
            rows_deleted = s2.run_sql(query_text)
            end_time = datetime.now()
            elapsed_time = end_time-start_time
            logging.info("remove false enrollments finished: {0}".format(end_time))
            logging.info("remove false enrollments elapsed time: {0}".format(elapsed_time))
            logging.info("{0} rows deleted".format(rows_deleted))
        except Exception as ex:
                logging.exception(ex)

    @staticmethod
    def prep_course_code(code):
        lower_letters = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", 
                        "p", "q", "r", "s", "t", "u", "v", "w", "x","y", "z"]
        if code.startswith("X-"):
                code = code.replace("X-", "")
        for letter in lower_letters:
                if code.endswith(letter):
                    code = code.replace(letter, "")
        return code

    @staticmethod
    def distribute_schema_design(s2conf):
        try:
                s2 = singlestore(s2conf)
                start_time = datetime.now()
                logging.info("distribute schema design started: {0}".format(start_time))
                course_code_list = ["CUST-SP-DEV-100", 
                                    "CUST-SP-OV-110", 
                                    "CUST-SP-DEV-110", 
                                    "CUST-SP-DEV-120"]
                course_name_list = ["Storage", 
                                    "Architecture Overview", 
                                    "Sharding and Shard Keys", 
                                    "Indexes"]
                course_id_list = [630, 608, 633, 616]
                query_text = "select * from enrollment where code = 'X-CUST-SP-DEV-105';"
                records = []
                records = s2.run_select(query_text)
                rows_inserted = 0
                for record in records:
                    record_count = s2.get_number_of_course_records(record[2], course_code_list[0])
                    if record_count[0][0] == 0:
                        for x in range(4):
                            insert_text = """insert into enrollment \
                                (course_id, user_id, subscribed_by, name, \
                                type, status, location_name, webinar_tool, session_date_begin,\
                                session_time_begin, session_timezone, code, course_begin_date, \
                                enroll_date_of_enrollment, enroll_begin_date, course_end_date, \
                                enroll_end_date, course_complete_date, \
                                score, total_time) \
                            values({0},{1},{2},'{3}','{4}','{5}','{6}','{7}','{8}','{9}', \
                            '{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}',{18},{19});"""\
                            .format(course_id_list[x], record[1],record[2], 
                                    course_name_list[x], record[4], record[5], 
                                    record[6], record[7], 
                                    docebo_etl.get_date(record[8]), '00:00:00', 
                                    record[10], course_code_list[x], 
                                    docebo_etl.get_datetime(record[12]), docebo_etl.get_datetime(record[13]), 
                                    docebo_etl.get_datetime(record[14]), docebo_etl.get_datetime(record[15]),
                                    docebo_etl.get_datetime(record[16]), docebo_etl.get_datetime(record[17]),
                                    record[18], record[19])
                            insert_text = insert_text.replace('None',"NULL")
                            num_rows = s2.run_sql(insert_text)
                            if type(num_rows) == int:
                                rows_inserted = rows_inserted + num_rows
                query_text = "delete from enrollment where code='X-CUST-SP-DEV-105'"
                rows_deleted = s2.run_sql(query_text)
                end_time = datetime.now()
                elapsed_time = end_time-start_time
                logging.info("distribute schema design finished: {0}".format(end_time))
                logging.info("distribute schema elapsed time: {0}".format(elapsed_time))
                logging.info("{0} rows deleted".format(rows_deleted))
                logging.info("{0} rows inserted".format(rows_inserted))
        except Exception as ex:
                logging.exception(ex)
    
    @staticmethod            
    def get_datetime(value_in):
        if type(value_in) == datetime:
            sql_dt = value_in.strftime('%Y-%m-%d %H:%M:%S')
            return sql_dt
        else:
            return '1900-01-01 00:00:00'
    @staticmethod    
    def get_date(value_in):
        if value_in != None:
            sql_dt = value_in.strftime('%Y-%m-%d')
            return sql_dt
        else:
            return '1900-01-01'
        
    @staticmethod
    def get_time(value_in):
        if value_in != None:
            sql_dt = value_in.strftime('%H:%M:%S')
        else:
            return '00:00:00'
           
