import docebo_types

class singlestore_helper:
    def __init__(self):
        pass
        
    def build_sql(self, stem, objects):
        if type(objects)==list:
            if type(objects[0] == docebo_types.course):
                final_query = stem
                for item in objects:
                    final_query = "{0} ({1},'{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',{11}),".format(final_query ,item.id, item.name, item.uid_course, item.date_last_updated, item.course_type, item.selling, item.code, item.price, item.start_date, item.end_date, item.duration)
                final_query = final_query[:-1]
                final_query = final_query + ";"
                return final_query

                    
    