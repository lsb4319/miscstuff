from threading import Thread, Lock
import logging
from datetime import datetime
import os
import math
from docebo_helper import docebo_helper
from singlestore import singlestore, s2_config
import time
import json
from singlestore_helper import singlestore_helper as s2helper

class docebo_data:
    def __init__(self, token, os_in):
        self.token = token
        self.headers = {"authorization": "Bearer {0}".format(self.token)}
        self.base_endpoint = "https://memsql.docebosaas.com/learn/v1"
        self.course_endpoint = "https://memsql.docebosaas.com/learn/v1/courses"
        self.survey_endpoint = "https://memsql.docebosaas.com/learn/v1/survey"
        self.enrollment_endpoint = "https://memsql.docebosaas.com/learn/v1/enrollments"
        self.user_endpoint = "https://memsql.docebosaas.com/manage/v1/user"
        self.s2apikey = "a9245ff05f5e3e1b9793d3cde55ec4777fc5c46240f4894041cbab7dc7120575"
        self.page_count = 1
        self.has_more_data = True
        self.s2conf = s2_config(os_in)
        self.error_count = 0
        self.lock = Lock()
        self.done = False
        self.s2 = singlestore(self.s2conf)
        self.total_pages = 0
      
#uses courses API to get all courses and insert them into the courses table.
    def get_courses(self, docebo_session,s2conf):
        try:
            start_time = datetime.now()
            logging.info("get courses started. {0}".format(start_time))
            request_data={'page':1, "page_size":1}
            courses = docebo_session.get(self.course_endpoint, data=request_data, 
                                            headers=self.headers).json()
            data = courses["data"]
            total_pages = math.ceil(data["total_page_count"]/20)
            while self.page_count <= total_pages:
                thread_list = list()
                for _ in range(10):
                    worker_thread = Thread(target=self.get_courses_worker, 
                                            args = (self.page_count, docebo_session, 
                                                    s2conf))
                    self.lock.acquire()
                    self.page_count += 1
                    self.lock.release()
                    thread_list.append(worker_thread)
                for x in thread_list:
                        x.start()
                for x in thread_list:
                        x.join()
                thread_list.clear()
            end_time = datetime.now()
            logging.info("get courses completed. {0}".format(end_time))
            elapsed_time = end_time - start_time
            logging.info("get courses elapsed time: {0}".format(elapsed_time))
            s2 = singlestore(s2conf)
            records = s2.get_number_of_records("course")
            logging.info("{0} records written to table 'course'.".format(records))
        except Exception as ex:
            logging.exception(ex)

    def get_courses_worker(self, page_number, docebo_session, s2conf):
        try:
            self.lock.acquire()
            request_data = {'page': page_number, 'page_size': 20}
            self.lock.release()
            courses = docebo_session.get(self.course_endpoint, 
                                         data=request_data, 
                                         headers=self.headers).json()
            data = courses['data']
            items = data['items']
            s2 = singlestore(s2conf)
            for item in items:
                del item["description"]
                json_data = json.dumps(item)
                json_data = json_data.replace("'","")
                s2.run_sql("call sp_ingest_course('{0}')".format(json_data))
        except Exception as ex:
            logging.exception(ex)

      #Uses the Surveys API to get the survey objects and add them to the surveys table
    def get_surveys(self, docebo_session, s2conf):
        try:
            start_time = datetime.now()
            logging.info("get_surveys started. {0}".format(start_time))
            self.page_count = 1
            query_text = 'select id from course;'
            s2=singlestore(s2conf)
            courses = s2.run_select(query_text)
            course_ids = []
            for course in courses:
                course_ids.append(course[0])
            thread_list = list()
            total_courses = len(course_ids)
            course_number = 0
            while course_number < total_courses:
                for _ in range(10):
                    if course_number < total_courses:
                        crs = course_ids[course_number]
                        worker_thread = Thread(target=self.get_surveys_worker, 
                                                args = (crs ,
                                                        docebo_session, 
                                                        s2conf))
                        course_number += 1
                        thread_list.append(worker_thread)
                    else:
                        pass
                for x in thread_list:
                    x.start()
                    time.sleep(.1)
                for x in thread_list:
                    x.join()
                thread_list.clear()
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            logging.info("get_surveys finished at {0}".format(end_time))
            logging.info("get surveys elapsed time: {0}".format(elapsed_time))
            logging.info("{0} records written to table 'survey'.".format(
                s2.get_number_of_records("survey")))
        except Exception as ex:
                logging.exception(ex)

    def get_surveys_worker(self, course, docebo_session, s2conf):
        try:
            page_count = self.page_count
            request_data = {'page': page_count, 'page_size': 200}
            course_id = course
            course_id = str(course).replace("(", "")
            course_id = course_id.replace(")", "")
            course_id = course_id.replace(",", "")
            request_endpoint = "{0}/{1}".format(self.course_endpoint, course_id)
            los = docebo_session.get(request_endpoint, 
                                        data=request_data, 
                                        headers=self.headers).json()
            query_text = "insert into survey (course_id, survey_id) values"
            query_values = ""
            for lo in los['data']['tree']:
                if lo['type'] == "poll":
                        survey_id = (lo['resource_id'])
                        insert_text = "({0}, {1})".format(course_id, survey_id)
                        query_values = "{0} {1}".format(query_values, insert_text)
                if query_values != "":
                        final_query = "{0}{1}".format(query_text, query_values)
                        final_query = docebo_helper.clean_query(self,final_query)
                        s2=singlestore(s2conf)
                        s2.run_sql(final_query)
        except Exception as ex:
                logging.exception(ex)

    #Method that iterates through all of the surveys and gets the unique questions asked in 
    #surveys. Stores them in the Questions table.
    def get_questions(self, docebo_session, s2conf):
        try:
            start_time = datetime.now()
            logging.info("get_questions started. {0}".format(start_time))
            query_text = "select course_id, survey_id from survey;"
            s2 = singlestore(s2conf)
            result = s2.run_select(query_text)
            survey_list = []
            for survey in result:
                survey_list.append(survey)
            total_surveys = len(survey_list)
            thread_list = list()
            survey_count = 0
            while survey_count < total_surveys:
                for x in range(10):
                    worker_thread = Thread(target=self.get_questions_worker, 
                                            args=(survey_list[x],
                                                    docebo_session, 
                                                    s2conf) )
                    survey_count += 1
                    thread_list.append(worker_thread)
                for x in thread_list:
                        x.start()
                        time.sleep(.1)
                for x in thread_list:
                        x.join()
                        thread_list.clear()
            end_time = datetime.now()
            elapsed_time = end_time-start_time
            logging.info("get questions completed at {0}".format(end_time))
            logging.info("get questions elapsed time  = {0}".format(elapsed_time))
            logging.info("{0} records written to table 'question'.".format
                            (s2.get_number_of_records("question")))
        except Exception as ex:
                logging.exception(ex)

    def get_questions_worker(self, survey, docebo_session, s2conf):
        try:
            course_id = survey[0]
            survey_id = survey[1]
            request_data = {"": ""}
            request_endpoint = "{0}/{1}/answer?id_course={2}".format(self.survey_endpoint, 
                                                                        survey_id, 
                                                                        course_id)
            temp = docebo_session.get(request_endpoint, 
                                        data=request_data, 
                                        headers=self.headers).json()
            questions = temp['data']['questions']
            for question in questions:
                q = temp['data']['questions'][question]
                if q['type_quest'] == "likert_scale":
                    q_type = q['type_quest']
                    title_temp = q['title_quest']
                    key_list = list(title_temp.keys())
                    val_list = list(title_temp.values())
                    i = 0
                    for key in key_list:
                        try: 
                            q_id = key
                            q_text = val_list[i]
                            q_text = q_text.split("/")[1]
                            insert_text = "insert ignore into question values({0},\'{1}\',\'{2}\');"\
                                .format(q_id, q_text, q_type)
                            s2=singlestore(s2conf)
                            s2.run_sql(insert_text)
                        except:
                                pass
                        i = i + 1
                else:
                    q_id = q['id_quest']
                    if not docebo_helper.check_list_for_item(self,questions, q_id):
                        q_text = q['title_quest']
                        q_type = q['type_quest']
                        insert_text = "insert ignore into question values({0},\'{1}\',\'{2}\');"\
                            .format(q_id, q_text, q_type)
                        s2 = singlestore(s2conf)
                        s2.run_sql(insert_text)
        except Exception as ex:
                logging.exception(ex)

      #Method that Uses the course api to get the answers to survey questions, 
      #corolates them to questions and courses and populates the answers table
    def get_answers(self, docebo_session, s2conf):
        try:
            start_time = datetime.now()
            logging.info("get answers started. {0}".format(start_time))
            query_text = "select course_id, survey_id from survey;"
            s2 = singlestore(s2conf)
            result = s2.run_select(query_text)
            survey_list = []
            for survey in result:
                survey_list.append(survey)
            total_surveys = len(survey_list)
            thread_list = list()
            survey_count = 0
            while survey_count < total_surveys:
                for x in range(10):
                    if survey_count < total_surveys:
                        worker_thread = Thread(target=self.get_answers_worker, 
                                                args=(survey_list[survey_count],
                                                        docebo_session, 
                                                        
                                                        s2conf) )
                        survey_count += 1
                        thread_list.append(worker_thread)
                for x in thread_list:
                        x.start()
                        time.sleep(.1)
                for x in thread_list:
                        x.join()
                        thread_list.clear()
            end_time = datetime.now()
            elapsed_time = end_time-start_time
            logging.info("get answers completed at {0}".format(end_time))
            logging.info("get answers elapsed time  = {0}".format(elapsed_time))
            logging.info("{0} records written to table 'answer'.".format
                            (s2.get_number_of_records("answer", )))
        except Exception as ex:
                logging.exception(ex)

    def get_answers_worker(self, survey, docebo_session, s2conf):
        try:
            course_id = survey[0]
            survey_id = survey[1]
            self.lock.acquire()
            page_no = 1
            self.lock.release()
            value_list = list()
            done=False
            records_inserted=0
            while not done:
                request_data = {"page": str(page_no),"page_size":200}
                request_endpoint = "{0}/{1}/answer?id_course={2};".format(self.survey_endpoint, 
                                                                            survey_id, 
                                                                            course_id)
                temp = docebo_session.get(request_endpoint, 
                                            data=request_data, 
                                            headers=self.headers).json()
                try:
                    self.total_pages = temp['data']['total_pages']
                    answers = temp['data']['answers']
                    self.lock.acquire()
                    page_no+=1
                    self.lock.release()
                    for answer in answers:
                        a = answer["answers"]
                        key_list = list(a.keys())
                        query_stub = "insert into answer values"
                        for key in key_list:
                            bar = answer["answers"][key]
                            if type(bar) == list and len(bar)!=0:
                                a_id = key
                                a_text = bar[0]
                                a_text = docebo_helper.clean_text(self,a_text)
                                a_date = answer['date']
                                if a_date == '0000-00-00 00:00:00':
                                    a_date = '1000-01-01 00:00:00'
                                a_likert_value = docebo_helper.get_likert_value(self,a_text)
                                value_list.append("({0},\'{1}\',{2},{3},\'{4}\',{5})".format
                                                (a_id, 
                                                a_text, 
                                                course_id, 
                                                survey_id, 
                                                a_date, 
                                                a_likert_value))
                            elif type(bar) == dict and len(bar)!=0:
                                bar_keys = list(bar.keys())
                                if bar_keys[0] != '':
                                    a_id = bar_keys[0]
                                    a_text = bar[a_id]
                                    a_text = docebo_helper.clean_text(self,a_text)
                                    a_date = answer['date']
                                    if a_date == '0000-00-00 00:00:00':
                                            a_date = '1000-01-01 00:00:00'
                                    a_likert_value = docebo_helper.get_likert_value(self,a_text)
                                    value_list.append("({0},\'{1}\',{2},{3},\'{4}\',{5})".format
                                                    (a_id, 
                                                    a_text, 
                                                    course_id, 
                                                    survey_id, 
                                                    a_date, 
                                                    a_likert_value))
                except KeyError as ex:
                    pass
                if len(value_list) != 0:
                        query_text = query_stub
                        for x in value_list:
                            query_text = query_text + x + ","
                        query_text = docebo_helper.clean_query(self,query_text)
                        s2 = singlestore(s2conf)
                        s2.run_sql(query_text)
                        self.lock.acquire()
                        query_text = ""
                        self.lock.release()
                self.lock.acquire()
                if page_no >= self.total_pages:
                        done = True
                self.lock.release()
            return records_inserted
        except Exception as ex:
                logging.exception(ex)

      #Method that gets user enrollments in courses and writes them to the enrollments table.    
    def get_enrollments(self, docebo_session, s2conf):
        try:
                s2 = singlestore(s2conf)
                start_time = datetime.now()
                logging.info("get_enrollments started. {0}".format(start_time))
                endpoint = "{0}?page={1}&page_size={1}".format(self.enrollment_endpoint,1,1)
                enrollments = docebo_session.get(endpoint, headers=self.headers).json()
                data = enrollments["data"]
                total_pages = data["total_page_count"]/200
                self.page_count = 1
                while self.page_count < total_pages:
                    thread_list = list()
                    for _ in range(10):
                            worker_thread = Thread(target=self.get_enrollments_worker, args = (self.page_count, docebo_session,s2conf))
                            self.lock.acquire()
                            self.page_count += 1
                            self.lock.release()
                            thread_list.append(worker_thread)
                    for x in thread_list:
                            x.start()
                    for x in thread_list:
                            x.join()
                end_time = datetime.now()
                elapsed_time = end_time - start_time
                logging.info("get_enrollments completed. {0}".format(end_time))
                logging.info("elapsed_time: {0}".format(elapsed_time))
                logging.info("{0} records written to table 'enrolment_ingest'.".format
                             (s2.get_number_of_records("enrollment",)))
        except Exception as ex:
                logging.exception(ex)

    def get_enrollments_worker(self, page_number, docebo_session, s2conf):
        try:
                endpoint = "{0}?page={1}&page_size={2}".format(self.enrollment_endpoint,page_number,200)
                temp = docebo_session.get(endpoint, headers=self.headers).json()
                enrollments = temp['data']['items']
                query_text = "insert ignore into enrollment(\
                              course_id, \
                              user_id, \
                              subscribed_by, \
                              name, \
                              type, \
                              status, \
                              location_name, \
                              webinar_tool, \
                              session_date_begin, \
                              session_time_begin, \
                              session_timezone, \
                              code, \
                              course_begin_date, \
                              enroll_date_of_enrollment, \
                              enroll_begin_date, \
                              course_end_date, \
                              enroll_end_date, \
                              course_complete_date, \
                              score, \
                              total_time)\
                              values"
                final_query = ""
                query_values = ""
                keys_to_keep = ["id",\
                                "user_id",\
                                "subscribed_by",\
                                "name", \
                                "type", \
                                "status",\
                                "location_name", \
                                "webinar_tool", \
                                "session_date_begin", \
                                "session_time_begin", \
                                "session_timezone", \
                                "code", \
                                "course_begin_date", \
                                "enroll_date_of_enrollment", \
                                "enroll_begin_date", \
                                "course_end_date", \
                                "enroll_end_date",\
                                "course_complete_date", \
                                "score", \
                                "total_time"]
                for enrollment in enrollments:  
                        temp_enroll = docebo_helper.filter_json(enrollment, keys_to_keep)
                        query_values = "{0}({1},{2},\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\"\
                            ,\"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",\"{16}\",\
                            \"{17}\",\"{18}\",{19},{20}),".format(
                                query_values, 
                                temp_enroll["id"],
                                temp_enroll["user_id"],
                                temp_enroll["subscribed_by"],
                                temp_enroll["name"],
                                temp_enroll["type"], 
                                temp_enroll["status"],
                                temp_enroll["location_name"],
                                temp_enroll["webinar_tool"],
                                temp_enroll["session_date_begin"], 
                                temp_enroll["session_time_begin"],
                                temp_enroll["session_timezone"],
                                temp_enroll["code"],
                                temp_enroll["course_begin_date"],
                                temp_enroll["enroll_date_of_enrollment"],
                                temp_enroll["enroll_begin_date"],
                                temp_enroll["course_end_date"],
                                temp_enroll["enroll_end_date"],
                                temp_enroll["course_complete_date"],
                                temp_enroll["score"], 
                                temp_enroll["total_time"])
                if len(query_values) >= 1:
                    final_query = "{0}{1}".format(query_text,query_values)
                    final_query = final_query.rstrip(",")
                    final_query = final_query + ";"
                    final_query = final_query.replace("None", "NULL")
                    s2=singlestore(s2conf)
                    if final_query != "":
                        s2.run_sql(final_query)
        except Exception as ex:
                logging.exception(ex)
                  
    def get_users(self, docebo_session, s2conf):
        try:
                s2 = singlestore(s2conf)
                start_time = datetime.now()
                logging.info("get_users started. {0}".format(start_time))
                endpoint = "{0}?page={1}&page_size={2}".format(self.user_endpoint,1,1)
                users = docebo_session.get(endpoint, headers=self.headers).json()
                data = users["data"]
                total_pages = data["total_page_count"]/200
                self.page_count = 1
                while self.page_count < total_pages:
                    thread_list = list()
                    for _ in range(10):
                            worker_thread = Thread(target=self.get_users_worker, 
                                                   args = (self.page_count,
                                                           docebo_session, 
                                                           
                                                           s2conf))
                            self.lock.acquire()
                            self.page_count += 1
                            self.lock.release()
                            thread_list.append(worker_thread)
                    for x in thread_list:
                            x.start()
                    for x in thread_list:
                            x.join()
                end_time = datetime.now()
                elapsed_time = end_time - start_time
                logging.info("get_users completed. {0}".format(end_time))
                logging.info("elapsed_time: {0}".format(elapsed_time))
                logging.info("{0} records written to table 'user'.".format
                             (s2.get_number_of_records("user",)))
        except Exception as ex:
                logging.exception(ex)
      
    def get_users_worker(self, page_number, docebo_session, s2conf):
        try:
            endpoint = "{0}?page={1}&page_size={2}".format(self.user_endpoint,
                                                           page_number,200)
            temp = docebo_session.get(endpoint, headers=self.headers).json()
            users = temp['data']['items']
            query_text = "insert ignore into user values"
            final_query = ""
            query_values = ""
            keys_to_keep = ["user_id","email", "uuid","last_access_date","status","level","timezone"]
            for user in users:  
                    temp_user = docebo_helper.filter_json(user, keys_to_keep)
                    email_domain = docebo_helper.get_email_domain(temp_user["email"])
                    is_singlestore = docebo_helper.get_is_singlestore(temp_user["email"])
                    query_values = "{0}({1},\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\"),".\
                        format(query_values, temp_user["user_id"], email_domain,temp_user["uuid"],\
                        temp_user["last_access_date"],temp_user["status"], temp_user["level"], \
                        temp_user["timezone"], is_singlestore)
            final_query = "{0}{1}".format(query_text,query_values)
            final_query = final_query.rstrip(",")
            final_query = final_query + ";"
            final_query = final_query.replace("None", "NULL")
            s2=singlestore(s2conf)
            if final_query != "insert ignore into user values;":
                s2.run_sql(final_query)
        except Exception as ex:
                logging.exception(ex)