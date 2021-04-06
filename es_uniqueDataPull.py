# Pulls Passwords and Usernames from Lookouts DB and puts them in common formats
from pprint import pprint
from datetime import datetime
from tqdm import tqdm

from Library import libES_Query
#from Library import libGitPush
from Library import libReport
from os import path

# Dependencies:
import yaml
# Press the green button in the gutter to run the script.

if __name__ == '__main__':

   YAMLFILE = "./ES_DataPull.yaml"  # System Configuration and Variables
   queryResults = {}

   if (path.exists(YAMLFILE)):
      # -- Loads Configuration File for LookOut --
      with open(YAMLFILE, "r") as file:
         dataPull_Config = yaml.load(file, Loader=yaml.FullLoader)
   else:
      print("ERROR: No config file, please refer to ES_DataPull.yml.example in root folder of script")
      exit()

   startTime=datetime.now()
   OUTPUT_FOLDER= dataPull_Config['OUTPUT_FOLDER']
   #OUTPUT_FOLDER="/Users/darrellmiller/Dropbox/Fusion Projects/Current Projects/cyber-intelligence-lists/"

   esQueryObj=libES_Query.es_query(ES_HOST=dataPull_Config['ES_HOST'], ES_INDEX=dataPull_Config['ES_INDEX'],
                                   ES_USERNAME=dataPull_Config['ES_USER'], ES_PASSWORD=dataPull_Config['ES_PASSWORD'])

   todays_date = datetime.now()
   hour = todays_date.hour
   day = datetime.today().strftime('%A')
   day_no = datetime.today().strftime('%d')
   print ("HOUR:", hour)

   for item in dataPull_Config['ES_FIELDLIST_HOURLY']:
      print ("Querying: ", item)
      esQueryObj.PullUniques(field_name=item)
      esQueryObj.saveUniques(OUTPUT_FOLDER, dataPull_Config['IGNORE_LIST'])

   #hour=00 #for testing
   if hour==1:
      for item in dataPull_Config['ES_FIELDLIST_DAILY']:
         print("Querying: ", item)
         esQueryObj.PullUniques(field_name=item)
         esQueryObj.saveUniques(OUTPUT_FOLDER, dataPull_Config['IGNORE_LIST'])

   if day == "Monday":
      for item in dataPull_Config['ES_FIELDLIST_WEEKLY']:
         print("Querying: ", item)
         esQueryObj.PullUniques(field_name=item)
         esQueryObj.saveUniques(OUTPUT_FOLDER, dataPull_Config['IGNORE_LIST'])

   if day_no == "01":
      for item in dataPull_Config['ES_FIELDLIST_MONTHLY']:
         print("Querying: ", item)
         esQueryObj.PullUniques(field_name=item)
         esQueryObj.saveUniques(OUTPUT_FOLDER, dataPull_Config['IGNORE_LIST'])

   print ("Time Taken:", datetime.now()-startTime)