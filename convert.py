import json
from google.cloud import pubsub_v1
import glob
import os

# Set up credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Pub/Sub configuration
project_id = "cc-project-milestone-1"
subscription_name = "transfer-sub"
convert_topic_name = "convert"

# Initialize clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, subscription_name)
convert_topic_path = publisher.topic_path(project_id, convert_topic_name)

def convert_units(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        
        if 'temperature' in data:
            data['temperature'] = round((data['temperature'] * 9/5) + 32, 2)
        
        if 'pressure' in data:
            data['pressure'] = round(data['pressure'] * 0.145038, 2)
        
        record = json.dumps(data).encode('utf-8')
        future = publisher.publish(convert_topic_path, record)
        future.result()
        print(f"Converted and published: {data}")
        message.ack()
        
    except json.JSONDecodeError:
        print("Invalid JSON format, acking message")
        message.ack()
    except KeyError as e:
        print(f"Missing required field {e}, acking message")
        message.ack()
    except Exception as e:
        print(f"Conversion error: {e}")
        message.nack()

streaming_pull = subscriber.subscribe(subscription_path, callback=convert_units)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("Stopped conversion service")
