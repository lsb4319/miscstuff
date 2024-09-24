from datetime import datetime
from docebo_auth import Docebo_Authentication
from singlestore import s2_config, singlestore
from docebo_helper import docebo_helper, os, mode
import docebo_data
import requests
import logging
import requests
from docebo_auth import Docebo_Authentication
from docebo_data import docebo_data
from docebo_etl import docebo_etl
import sys
import docebo_time
import time
import pdb

def run(os_in, mode_in):
    try:
        total_start_time = datetime.now()
        logging.info("Get Docebo Data started: {0}".format(total_start_time))
        docebo_session = requests.Session()
        da = Docebo_Authentication()
        token = da.get_token()
        docebo_session.headers.update({'authorization': 'Bearer {0}'.format(token)})
        dd = docebo_data(token, os_in)
        s2conf = s2_config(os_in)
        s2 = singlestore(s2conf)
        s2.delete_records("course")
        dd.get_courses(docebo_session, s2conf)
        s2.delete_records("survey")
        dd.get_surveys(docebo_session, s2conf)
        s2.delete_records("question")
        dd.get_questions(docebo_session, s2conf)
        s2.delete_records("answer")
        dd.get_answers(docebo_session, s2conf)
        s2.delete_records("enrollment")
        dd.get_enrollments(docebo_session, s2conf)
        s2.delete_records("user")
        dd.get_users(docebo_session, s2conf)
        docebo_etl.group_courses(s2conf)
        docebo_etl.distribute_schema_design(s2conf)
        docebo_etl.remove_false_enrollments(s2conf)
        total_end_time = datetime.now()
        logging.info("Get Docebo Data finished: {0}".format(total_end_time))
        elapsed_time = total_end_time - total_start_time
        logging.info("Total Elapsed time: {0}".format(elapsed_time))
    except Exception as ex:
        logging.exception(ex)
        
        
def main():
    args = docebo_helper.get_args(sys.argv)
    client_os = args[0]
    run_mode = args[1]
    if run_mode == mode.service:
        td = docebo_time.target_date(client_os)
        temp_target = td.get_target()
        td.set_time(temp_target)
        while True:
            logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
            logger = logging.getLogger()
            log_time = str(datetime.now())
            log_time = log_time.replace(" ","_")
            log_time = log_time.replace(":","-")
            if client_os == os.windows:
                log_file = ".\\logs\\{0}.log".format(log_time)
            elif client_os == os.linux:
                log_file = "logs/{0}.log".format(log_time)
            elif client_os == os.mac:
                log_file = "logs/{0}.log".format(log_time)
            handler = logging.FileHandler(log_file, mode='a')
            logger.addHandler(handler)
            logging.info("Run mode: {0}".format(run_mode))
            logging.info("OS = {0}".format(client_os))
            target = td.get_time()
            delta = target-datetime.now()
            delay = delta.total_seconds()
            logging.info("Sleeping for {0} seconds".format(delay))
            time.sleep(delay)
            run(client_os, mode.service)
            logger.removeHandler(handler)
            target = td.increment_time(target)
            td.set_time(target)
    elif run_mode == mode.directly:
        logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger()
        log_time = str(datetime.now())
        log_time = log_time.replace(" ","_")
        log_time = log_time.replace(":","-")
        if client_os == os.windows:
            log_file = ".\\logs\\{0}.log".format(log_time)
        elif client_os == os.linux:
            log_file = "logs/{0}.log".format(log_time)
        elif client_os == os.mac:
            log_file = "logs/{0}.log".format(log_time)
        handler = logging.FileHandler(log_file, mode='a')
        logger.addHandler(handler)
        logging.info("Run mode: {0}".format(run_mode))
        logging.info("OS = {0}".format(client_os))
        run(client_os, mode.directly)
    elif run_mode == mode.debug:
        logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger()
        log_time = str(datetime.now())
        log_time = log_time.replace(" ","_")
        log_time = log_time.replace(":","-")
        if client_os == os.windows:
                log_file = ".\\logs\\{0}.log".format(log_time)
        elif client_os == os.linux:
            log_file = "logs/{0}.log".format(log_time)
        elif client_os == os.mac:
            log_file = "logs/{0}.log".format(log_time)        
        handler = logging.FileHandler(log_file, mode='a')
        logger.addHandler(handler)
        logging.info("Run mode: {0}".format(run_mode))
        logging.info("OS = {0}".format(client_os))
        run(client_os, mode.debug)
    
    
if __name__ == '__main__':
      main()
