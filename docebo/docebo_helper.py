import json
from datetime import datetime
import logging
import requests
import sys
from enum import Enum
import platform

class docebo_helper:
      def __init__(self) -> None:
            pass
      @staticmethod
      def get_min_date(dict_in):
          data = dict_in["data"]
          items = data["items"]
          date_list = []
          for item in items:
            date_list.append(item["date_last_updated"])
          date_list.sort()
          print(date_list[0])
          print(date_list[len(date_list)-1])
          print(date_list)

      
      @staticmethod
      def check_list_for_item(self, list_in, item):
            try:
                  list_in.index(item)
                  return True
            except:
                  return False

      @staticmethod
      def check_json_for_data(self, json_in):
            try:
                  j_in = json_in['data']
                  return True
            except:
                  return False

      @staticmethod
      def get_question_text(self, question_id, question_list):
            for question in question_list:
                  if question.question_id == question_id:
                        return question.question_text

      @staticmethod
      def get_likert_value(self, text_in):
            likert_text = str(text_in)
            text_in = likert_text.lower()
            if likert_text == "Strongly disagree":
                  return 1
            elif likert_text == "Disagree":
                  return 2
            elif likert_text == "Neither agree or disagree":
                  return 3
            elif likert_text == "Agree":
                  return 4
            elif likert_text == "Strongly agree":
                  return 5
            else:
                  return 0
      @staticmethod
      def clean_json(self,json_in):
            description = json_in["description"]
            description = description.replace("\"","")
            description = description.replace("\\","")
            json_in["description"] = description
            name = json_in["name"]
            name = name.replace("\"","")
            name = name.replace("\\","")
            json_in["name"] = name
            json_out = (json.dumps(json_in))
            return json_out

      @staticmethod
      def clean_query(self, query_in):
            query_out = query_in.replace("\\'","'")
            query_out = query_out.replace("<", "")
            query_out = query_out.replace(">","")
            query_out = query_out.replace("/", "")
            query_out = query_out.rstrip(",")
            return query_out

      @staticmethod
      def clean_json(self, json_in):
            description_text=json_in["description"]
            description_text=description_text.replace("\"","")
            description_text=description_text.replace("\\","")
            description_text=description_text.replace("\\\\","")
            description_text=description_text.replace("\'","")
            description_text=description_text.replace("<","")
            description_text=description_text.replace(">","")
            description_text=description_text.replace("'","")
            description_text=description_text.replace("/","")
            description_text=description_text.replace("\n","")
            json_in["description"] = description_text
            name_text=json_in["name"]
            name_text=name_text.replace("\"","")
            name_text=name_text.replace("\\","")
            name_text=name_text.replace("\\\\","")
            name_text=name_text.replace("\'","")
            name_text=name_text.replace("<","")
            name_text=name_text.replace(">","")
            name_text=name_text.replace("'","")
            name_text=name_text.replace("/","")
            name_text=name_text.replace("\n","")
            json_in["name"] = name_text
            try:
                  username_text=json_in["username"]
                  username_text=username_text.replace("\'","")
                  json_in["username"]=username_text
            except:
                  pass
            return_text = json.dumps(json_in)
            return return_text

      @staticmethod
      def clean_text(self, text_in):
            return_text=text_in
            return_text=return_text.replace("\"","")
            return_text=return_text.replace("\\","")
            return_text=return_text.replace("\\\\","")
            return_text=return_text.replace("\'","")
            return_text=return_text.replace("<","")
            return_text=return_text.replace(">","")
            return_text=return_text.replace("'","")
            return_text=return_text.replace("/","")
            return_text=return_text.replace("\n","")
            return return_text

      
      def filter_json(json_obj, keys_to_keep):
            if isinstance(json_obj,dict):
                  for key in list(json_obj.keys()):
                        if key not in keys_to_keep:
                              del json_obj[key]
            return json_obj
        
      def get_email_domain(email):
            email = str(email)
            x = email.split("@")
            return x[1]
        
      def get_is_singlestore(email):
            email = str(email)
            x = email.split("@")
            if x[1] == "singlestore.com" or x[1] == "memsql.com":
                return "SingleStore"
            else:
                return "Not SingleStore"
          
      def get_mode(mode_in):      
            correct = False
            while not correct:
                  try:
                        mode_in = int(mode_in[1])
                  except:
                        mode_in = 999
                  try:
                        correct = False
                        if mode_in == 0:
                              print("'mode' argument was not specified.")
                              correct = False
                        elif mode_in == 1:
                              correct = True
                              return mode.service
                        elif mode_in == 2:
                              correct == True
                              return mode.directly
                        elif mode_in == 3:
                              correct == True
                              return mode.debug
                        else:
                              correct == False
                              print("The mode argument supplyed is invalid")
                        print("Please select the mode that you wish to run in:")
                        print("1. As a Service")
                        print("2. Directly")
                        print("3. Debugging")
                        try:
                              selected_mode = int(input())
                        except:
                              selected_mode = 4
                              
                        if selected_mode == 1:
                              correct = True
                              return mode.service
                        elif selected_mode == 2:
                              correct = True
                              return mode.directly
                        elif selected_mode == 3:
                              correct = True
                              return mode.debug
                        else:
                              print("{0} is not a correct response. Please try again".format(selected_mode))
                              correct = False
                  except Exception as ex:
                        logging.exception(ex)
                        
      def get_args(args):
            if len(args)==1:
                  mode = docebo_helper.get_mode([0])
            elif len(args)==2:
                  mode = docebo_helper.get_mode(args)
            current_os = platform.system()
            if current_os=="Darwin":
                  my_os = os.mac
            elif current_os=="Windows":
                  my_os = os.windows
            elif current_os=="Linux":
                  my_os = os.linux
            return_value = [my_os,mode]
            return return_value
                  
class mode(Enum):
      service = 1
      directly = 2
      debug = 3

class os(Enum):
      mac = 1
      windows = 2
      linux = 3
      
def main():
    args = docebo_helper.get_args(sys.argv)
    print(args[0])
    print(args[1])
    
if __name__ == '__main__':
    main()