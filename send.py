#Data Frames
import pandas as pd;
import uuid
import datetime
import random
import json
# from azure.servicebus import ServiceBusService  #https://pypi.org/project/azure-servicebus/
from azure.servicebus.control_client import ServiceBusService
from azure.eventhub import EventHubClient, Sender, EventData
import time

sbs = ServiceBusService(service_namespace='eventhubmoz', shared_access_key_name='RootManageSharedAccessKey', shared_access_key_value='airtg3xwlWxFoVbphuDahZipG3oBrc3OjC70L45Hil0=')

# readcsv= pd.read_csv("C://Users//User//Downloads//hive//power_bi//Power BI Project//AzureEventHub_df.csv");
readcsv= pd.read_csv("C:\\Users\\User\\Downloads\\hive\\power_bi\\Power BI Project\\AzureEventHub_df.csv");
my_df = pd.DataFrame(readcsv);
#print(my_df);

length = len(my_df);
print("my_df.len: ",length );
#print("my_df.loc: ",my_df.iloc[1]);

for y in range(0,10):
    for i in range (0,length):
        # for sensor 1
        reading = {'id': int(my_df.iloc[i,1]), 'timestamp': str(datetime.datetime.utcnow()),'sensor': int(my_df.iloc[i,2]),  'uv': float(my_df.iloc[i,5]), 'temperature': int(my_df.iloc[i,3]), 'humidity': int(my_df.iloc[i,0])}
        s = json.dumps(reading)
        sbs.send_event('eventhubmoz', s)
        print(reading)
        time.sleep(2);

        # for sensor 2
        reading = {'id': (int(my_df.iloc[i,1]) + 1), 'timestamp': str(datetime.datetime.utcnow()),'sensor': 2,  'uv': (float(my_df.iloc[i,5])+0.2), 'temperature': (int(my_df.iloc[i,3])+2), 'humidity': (int(my_df.iloc[i,0])+2)}
        s = json.dumps(reading)
        sbs.send_event('eventhubmoz', s)
        print(reading)
        time.sleep(2);
    print (y);