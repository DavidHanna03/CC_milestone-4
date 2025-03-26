import json
from google.cloud import pubsub_v1
import glob
import os

# Set up Google Cloud credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Configure your project and topics/subscription
project_id = "cc-project-milestone-1"
subscription_name = "filter-sub"
convert_topic_name = "transfer"

# Initialize Pub/Sub clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Create paths for the subscription and target topic
subscription_path = subscriber.subscription_path(project_id, subscription_name)
convert_topic_path = publisher.topic_path(project_id, convert_topic_name)

def process_message(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        
        if all(data.get(field) is not None for field in ['temperature', 'humidity', 'pressure']):
            record = json.dumps(data).encode('utf-8')
            future = publisher.publish(convert_topic_path, record)
            future.result()
            print(f"Published to 'convert': {data}")
            message.ack()
        else:
            print(f"Filtered out invalid message: {data}")
            message.ack()
    
    except json.JSONDecodeError:
        print("Invalid JSON received. Acking message.")
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

streaming_pull = subscriber.subscribe(subscription_path, callback=process_message)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("Stopped listening.")
