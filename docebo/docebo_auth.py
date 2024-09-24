import requests
import json
import logging

class Docebo_Authentication:
      def __init__(self):
            self.consumer_key = "training"
            self.consumer_secret = "ae3144569404d2ba883422ac7fef20e981d9ddf6"
            self.username = "lsmithbates@singlestore.com"
            self.password = "jSB^jR8FI9i$iY9u"

      def get_token(self):
            try:
                  payload = {
                  'grant_type': 'password',
                  'client_id': self.consumer_key,
                  'client_secret': self.consumer_secret,
                  'username': self.username,
                  'password': self.password
                  }
                  r = requests.post("https://memsql.docebosaas.com/oauth2/token", 
                  headers={"Content-Type":"application/x-www-form-urlencoded"},
                  data=payload)
                  r_json=json.loads(r.content)
                  token=r_json["access_token"]
                  return(token)
            except Exception as ex:
                  logging.exception(ex)
                  
def main():
    da = Docebo_Authentication()
    print(da.get_token())
    
if __name__ == '__main__':
    main()

