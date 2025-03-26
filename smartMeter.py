from google.cloud import pubsub_v1      # Google Cloud Pub/Sub client
import glob                             # For searching for JSON files 
import json
import os 
import random
import numpy as np                      # For generating random values using normal distribution
import time

# Locate the first .json file in the current directory (assumed to be the service account key)
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]  # Set the environment variable for GCP auth

# Define your GCP project ID and Pub/Sub topic name
project_id = "cc-project-milestone-1"
topic_name = "filter"

# Create a Pub/Sub publisher client and get the full topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# Define simulated sensor data profiles for different cities using normal distribution
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201)},
}
profileNames = ["boston", "denver", "losang"]

# Generate a random starting ID for sensor messages
ID = np.random.randint(0, 10000000)

# Infinite loop to simulate and publish sensor data
while True:
    # Randomly select a profile (city)
    profile_name = profileNames[random.randint(0, 2)]
    profile = DEVICE_PROFILES[profile_name]
    
    # Generate random sensor values based on the profile's normal distribution
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))
    
    # Construct the message dictionary
    msg = {
        "ID": ID,
        "time": int(time.time()),               # Unix timestamp
        "profile_name": profile_name,
        "temperature": temp,
        "humidity": humd,
        "pressure": pres
    }
    ID += 1  # Increment ID for next message
    
    # Introduce occasional missing data fields to simulate sensor faults
    if random.randrange(0, 10) < 1:
        msg['temperature'] = None
    if random.randrange(0, 10) < 1:
        msg['humidity'] = None
    if random.randrange(0, 10) < 1:
        msg['pressure'] = None
    
    # Convert message to JSON and encode to bytes
    record_value = json.dumps(msg).encode('utf-8')
    
    # Attempt to publish the message to Pub/Sub
    try:
        future = publisher.publish(topic_path, record_value)
        future.result()  # Wait for the publish to complete
        print("The message {} has been published successfully".format(msg))
    except:
        print("Failed to publish the message")
    
    time.sleep(0.5)  # Wait half a second before sending the next message
