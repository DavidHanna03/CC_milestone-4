from google.cloud import pubsub_v1      # pip install google-cloud-pubsub
import glob                             # for searching for json file 
import json
import os 
import random
import numpy as np                      # pip install numpy
import time

# Search the current directory for the JSON file (including the service account key) 
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "cc-project-milestone-1"
topic_name = "filter"

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201)},
}
profileNames = ["boston", "denver", "losang"]

ID = np.random.randint(0, 10000000)
while True:
    profile_name = profileNames[random.randint(0, 2)]
    profile = DEVICE_PROFILES[profile_name]
    
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))
    
    msg = {
        "ID": ID,
        "time": int(time.time()),
        "profile_name": profile_name,
        "temperature": temp,
        "humidity": humd,
        "pressure": pres
    }
    ID += 1
    
    if random.randrange(0, 10) < 1:
        msg['temperature'] = None
    if random.randrange(0, 10) < 1:
        msg['humidity'] = None
    if random.randrange(0, 10) < 1:
        msg['pressure'] = None
    
    record_value = json.dumps(msg).encode('utf-8')
    
    try:
        future = publisher.publish(topic_path, record_value)
        future.result()
        print("The message {} has been published successfully".format(msg))
    except:
        print("Failed to publish the message")
    
    time.sleep(0.5)
