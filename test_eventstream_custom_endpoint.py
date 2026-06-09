#!/usr/bin/env python
# coding: utf-8

# ## Eventstream test producer
#
# New notebook

# This notebook sends simulated data to a Fabric Eventstream through a custom
# (Event Hub compatible) endpoint. Use it to test your Eventstream
# configuration before connecting your real data sources.
#
# Before running:
# - In your Eventstream custom endpoint settings, copy the primary connection
#   string ("Connection string-primary key") and the Event Hub name.
# - Recommended: store both as secrets in Azure Key Vault and set USE_KEY_VAULT
#   to True (the values are then retrieved at runtime and never written into the
#   notebook). The account or identity running the notebook needs Get permission
#   on the vault.
# - Quick test only: set USE_KEY_VAULT to False and paste the values inline.
#   Do not commit or share the notebook with a real connection string in it.

# In[ ]:


# %pip and !pip behave the same in a Python notebook, but %pip is the
# recommended inline install for interactive Fabric notebook sessions.
get_ipython().run_line_magic('pip', 'install azure-eventhub')


# In[ ]:


import time
import json
import random
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData


# In[ ]:


# *** CONFIGURE THESE VALUES ***

# True  = retrieve the connection string and Event Hub name from Azure Key Vault.
# False = use the inline values below (quick test only).
USE_KEY_VAULT = True

# Option A (recommended): Azure Key Vault.
KEY_VAULT_URI = "https://YOUR-KEY-VAULT-NAME.vault.azure.net/"
CONNECTION_STRING_SECRET_NAME = "eventstream-connection-string"
EVENTHUB_NAME_SECRET_NAME = "eventstream-eventhub-name"

# Option B (quick test only): paste your values here.
CONNECTION_STRING_INLINE = "YOUR_PRIMARY_CONNECTION_STRING_HERE"
EVENTHUB_NAME_INLINE = "YOUR_EVENTHUB_NAME_HERE"

# How many messages to send. Set to None to run until you stop the cell.
MESSAGES_TO_SEND = 20

# Seconds to wait between messages.
SEND_INTERVAL_SECONDS = 5

# *** END OF CONFIGURATION ***


if USE_KEY_VAULT:
    # notebookutils is a built-in Fabric utility; no import is required.
    connection_str = notebookutils.credentials.getSecret(KEY_VAULT_URI, CONNECTION_STRING_SECRET_NAME)
    eventhub_name = notebookutils.credentials.getSecret(KEY_VAULT_URI, EVENTHUB_NAME_SECRET_NAME)
else:
    connection_str = CONNECTION_STRING_INLINE
    eventhub_name = EVENTHUB_NAME_INLINE
    print("WARNING: using an inline connection string. Do not commit or share this notebook with a real secret in it.\n")

# Fail fast with a clear message if a placeholder was left unchanged.
if not connection_str or connection_str == "YOUR_PRIMARY_CONNECTION_STRING_HERE":
    raise ValueError("Set your Eventstream connection string before running this notebook.")
if not eventhub_name or eventhub_name == "YOUR_EVENTHUB_NAME_HERE":
    raise ValueError("Set your Eventstream Event Hub name before running this notebook.")


# In[ ]:


sent_count = 0
print(f"Sending messages to Event Hub '{eventhub_name}'. Stop the cell at any time to halt.\n")

# The context manager guarantees the producer is closed even if an error occurs.
with EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name,
) as producer:
    try:
        while MESSAGES_TO_SEND is None or sent_count < MESSAGES_TO_SEND:
            # Example JSON payload with a random value and a UTC timestamp.
            data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "Test JSON message",
                "value": random.randint(1, 100),
            }
            json_message = json.dumps(data)

            # Build a single-event batch and tag it as JSON.
            event = EventData(json_message)
            event.content_type = "application/json"

            event_data_batch = producer.create_batch()
            event_data_batch.add(event)
            producer.send_batch(event_data_batch)

            sent_count += 1
            print(f"Sent ({sent_count}): {json_message}")

            # Skip the final wait so the loop ends promptly on the last message.
            if MESSAGES_TO_SEND is None or sent_count < MESSAGES_TO_SEND:
                time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped by user.")

print(f"\nFinished. {sent_count} message(s) sent.")