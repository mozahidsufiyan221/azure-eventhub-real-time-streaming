import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
# Data Frames
import pandas as pd;
import uuid
import datetime
import random
import json
import time
sbs = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eventhubmoz1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=sxHkDe/8hySz6/e0Siyn9M9cbE4Rq3u8OmnyZcdS230=", eventhub_name="eventhubmoz1")
readcsv = pd.read_csv("AzureEventHub_df.csv");
my_df = pd.DataFrame(readcsv);
# print(my_df);

length = len(my_df);
print("my_df.len: ", length);
# print("my_df.loc: ",my_df.iloc[1]);
# event_data_batch = sbs.create_batch()
for y in range(0, 10):
    for i in range(0, length):
        # for sensor 1
        reading = {'id': int(my_df.iloc[i, 1]), 'timestamp': str(datetime.datetime.utcnow()),
                   'sensor': int(my_df.iloc[i, 2]), 'uv': float(my_df.iloc[i, 5]), 'temperature': int(my_df.iloc[i, 3]),
                   'humidity': int(my_df.iloc[i, 0])}
        s = json.dumps(reading)
        # sbs.send_event('eventhubmoz', s)
        # event_data_batch = sbs.create_batch()
        # sbs.event_data_batch.add(s)
        print(s)
        time.sleep(2);


        async def run():
            # Create a producer client to send messages to the event hub.
            # Specify a connection string to your event hubs namespace and
            # the event hub name.
            producer = EventHubProducerClient.from_connection_string(
                conn_str="Endpoint=sb://eventhubmoz1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=sxHkDe/8hySz6/e0Siyn9M9cbE4Rq3u8OmnyZcdS230=",
                eventhub_name="eventhubmoz1")
            async with producer:
                # Create a batch.
                event_data_batch = await producer.create_batch()

                # Add events to the batch.
                event_data_batch.add(EventData(s))
                # event_data_batch.add(EventData('Second event'))
                # event_data_batch.add(EventData('Third event'))
                print("hello")

                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)


        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
    print(y);



# async def run():
#     # Create a producer client to send messages to the event hub.
#     # Specify a connection string to your event hubs namespace and
#     # the event hub name.
#     producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eventhubmoz.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=LDwwC/9XTQRi9MPlzxWSxFihgbUs7X1QQu7KPuHYFRk=", eventhub_name="eventhubmoz1")
#     async with producer:
#         # Create a batch.
#         event_data_batch = await producer.create_batch()
#
#         # Add events to the batch.
#         event_data_batch.add(EventData('First event '))
#         event_data_batch.add(EventData('Second event'))
#         event_data_batch.add(EventData('Third event'))
#         print("hello")
#
#         # Send the batch of events to the event hub.
#         await producer.send_batch(event_data_batch)
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(run())








# # Data Frames
# import pandas as pd;
# import uuid
# import datetime
# import random
# import json
# # from azure.servicebus import ServiceBusService  #https://pypi.org/project/azure-servicebus/
# from azure.servicebus.control_client import ServiceBusService
# from azure.eventhub import EventHubClient, Sender, EventData
# import time
#
# sbs = ServiceBusService(service_namespace='eventhubmoz', shared_access_key_name='RootManageSharedAccessKey',
#                         shared_access_key_value='airtg3xwlWxFoVbphuDahZipG3oBrc3OjC70L45Hil0=')
#
# # readcsv= pd.read_csv("C://Users//User//Downloads//hive//power_bi//Power BI Project//AzureEventHub_df.csv");
# readcsv = pd.read_csv("AzureEventHub_df.csv");
# my_df = pd.DataFrame(readcsv);
# # print(my_df);
#
# length = len(my_df);
# print("my_df.len: ", length);
# # print("my_df.loc: ",my_df.iloc[1]);
#
# for y in range(0, 10):
#     for i in range(0, length):
#         # for sensor 1
#         reading = {'id': int(my_df.iloc[i, 1]), 'timestamp': str(datetime.datetime.utcnow()),
#                    'sensor': int(my_df.iloc[i, 2]), 'uv': float(my_df.iloc[i, 5]), 'temperature': int(my_df.iloc[i, 3]),
#                    'humidity': int(my_df.iloc[i, 0])}
#         s = json.dumps(reading)
#         sbs.send_event('eventhubmoz', s)
#         print(reading)
#         time.sleep(2);
#
#         # for sensor 2
#         reading = {'id': (int(my_df.iloc[i, 1]) + 1), 'timestamp': str(datetime.datetime.utcnow()), 'sensor': 2,
#                    'uv': (float(my_df.iloc[i, 5]) + 0.2), 'temperature': (int(my_df.iloc[i, 3]) + 2),
#                    'humidity': (int(my_df.iloc[i, 0]) + 2)}
#         s = json.dumps(reading)
#         sbs.send_event('eventhubmoz', s)
#         print(reading)
#         time.sleep(2);
#     print(y);
