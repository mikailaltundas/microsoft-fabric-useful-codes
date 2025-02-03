#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# This notebook allows you to send data to your Evenstream using a custom endpoint. It is designed to help you test your Evenstream configuration with simulated data before integrating your actual data sources.
# 
# Before running the code, make sure to update the **connection_str** and **eventhub_name** variables with your own values. Replace **connection_str** with the primary connection string (the "Connection string-primary key") from your Evenstream settings, and set **eventhub_name** to your Evenstream's Event Hub name.

# In[9]:


get_ipython().system('pip install azure-eventhub')


# In[10]:


import time
import json
import random
from azure.eventhub import EventHubProducerClient, EventData


# In[11]:


# *** MODIFY THESE VARIABLES WITH YOUR OWN VALUES ***
# Replace the connection string below with your primary connection string
connection_str = "YOUR_PRIMARY_CONNECTION_STRING_HERE"

# Replace the event hub name below with your Event Hub's name
eventhub_name = "YOUR_EVENTHUB_NAME_HERE"
# *** END OF MODIFICATIONS ***


# In[12]:


# Create the Event Hub Producer client
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

try:
    while True:
        # Create a batch of events
        event_data_batch = producer.create_batch()

        # Example JSON-formatted data with a random value
        data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "message": "Test JSON message",
            "value": random.randint(1, 100)
        }
        # Convert the data to a JSON string
        json_message = json.dumps(data)
        
        # Add the JSON message to the event batch
        event_data_batch.add(EventData(json_message))
        print("Sending JSON message:", json_message)
        
        # Send the batch of events
        producer.send_batch(event_data_batch)
        
        # Pause for 5 seconds before sending the next message
        time.sleep(5)
finally:
    producer.close()

