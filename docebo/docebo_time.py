from datetime import datetime
import json
from docebo_helper import os, mode
import pdb

class target_date:
      def __init__(self, os_in) -> None:
            self.os = os_in
            
      def get_time(self):
            time_file = ""
            if self.os == os.windows:
                  time_file = open(".\\time.json","r")
            elif self.os == os.mac:
                  time_file = open("time.json", "r")
            elif self.os == os.linux:
                  time_file = open("time.json", "r")
            time_text = time_file.read()
            time_text = json.loads(time_text)
            year = time_text["year"]
            month = time_text["month"]
            day = time_text["day"]
            hour = time_text["hour"]
            minute = time_text["minute"]
            second = time_text["second"]
            target_time = datetime(year, month, day, hour, minute, second)
            time_file.close()
            return target_time
      
      def set_time(self,dt_in):
            if self.os == os.windows:
                  time_file = open(".\\time.json","w")
            elif self.os == os.mac:
                  time_file = open("time.json", "w")
            elif self.os == os.linux:
                  time_file = open("time.json", "w")
            dict_time = {
                  "year":dt_in.year,
                  "month":dt_in.month,
                  "day":dt_in.day,
                  "hour":dt_in.hour,
                  "minute":dt_in.minute,
                  "second":dt_in.second
                  }
            json_time = json.dumps(dict_time)
            time_file.write(str(json_time))
            time_file.close()
            
      def increment_time(self,dt_in):
            year = dt_in.year
            month = dt_in.month
            day = dt_in.day
            hour = dt_in.hour
            minute = dt_in.minute
            second = dt_in.second
            if day == 31 and (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10):
                day = 1
                month += 1
            elif day == 30 and (month == 4 or month == 6 or month == 9 or month == 11):
                day = 1
                month += 1
            elif day == 28 and month == 2 and (year != 2024 and year != 2028 and year != 2032 and year != 2036 and year != 2040 and year != 2044 and year != 2048 and year != 2052 and year != 2056 and year != 2060 and year != 2064 and year != 2068 and year != 2072 and year != 2076 and year != 2080 and year != 2084 and year != 2088 and year != 2092 and year != 2096):
                  day = 1
                  month += 1
            elif day == 29  and month == 2 and (year == 2024 or year == 2028 or year == 2032 or year == 2036 or year == 2040 or year == 2044 or year == 2048 or year == 2052 or year == 2056 or year == 2060 or year == 2064 or year == 2068 or year == 2072 or year == 2076 or year == 2080 or year == 2084 or year == 2088 or year == 2092 or year == 2096):
                        day = 1
                        month += 1
            elif day == 31 and month == 12:
                  day = 1
                  month = 1
                  year += 1
            else: 
                  day += 1
            return datetime(year, month, day, hour, minute, second)
      
      def get_target(self):
       #     pdb.set_trace()
            time_file = ""
            if self.os == os.windows:
                  time_file = open(".\\time.json","r")
            elif self.os == os.mac:
                  time_file = open("time.json", "r")
            elif self.os == os.linux:
                  time_file = open("time.json", "r")
            time_text = time_file.read()
            time_text = json.loads(time_text)
            return_value = datetime(time_text["year"],time_text["month"],time_text["day"],time_text["hour"],time_text["minute"],time_text["second"])
            return return_value