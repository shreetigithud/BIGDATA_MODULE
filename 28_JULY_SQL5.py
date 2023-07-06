#!/usr/bin/env python
# coding: utf-8

# In[ ]:


1. Setting up a Kafka Producer:
   a) Write a Python program to create a Kafka producer.
   b) Configure the producer to connect to a Kafka cluster.
   c) Implement logic to send messages to a Kafka topic.



# In[ ]:


from confluent_kafka import Producer

# Producer configuration
bootstrap_servers = 'localhost:9092'
topic = 'my-topic'

# Create producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Send messages to the Kafka topic
for i in range(10):
    message = f"Message {i}"
    producer.produce(topic, value=message.encode('utf-8'))

# Wait for message delivery
producer.flush()


# In[ ]:


2. Setting up a Kafka Consumer:
   a) Write a Python program to create a Kafka consumer.
   b) Configure the consumer to connect to a Kafka cluster.
   c) Implement logic to consume messages from a Kafka topic.



# In[ ]:


from confluent_kafka import Consumer, KafkaException

# Consumer configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'my-topic'

# Create consumer instance
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})

# Subscribe to topic
consumer.subscribe([topic])

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        # Process the message
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer
    consumer.close()


# In[ ]:


3. Creating and Managing Kafka Topics:
   a) Write a Python program to create a new Kafka topic.
   b) Implement functionality to list existing topics.
   c) Develop logic to delete an existing Kafka topic.



# In[ ]:


from confluent_kafka.admin import AdminClient, NewTopic

# AdminClient configuration
bootstrap_servers = 'localhost:9092'

# Create AdminClient instance
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Create a new topic
new_topic = NewTopic('my-topic', num_partitions=1, replication_factor=1)
admin_client.create_topics([new_topic])


# In[ ]:


4. Producing and Consuming Messages:
   a) Write a Python program to produce messages to a Kafka topic.
   b) Implement logic to consume messages from the same Kafka topic.
   c) Test the end-to-end flow of message production and consumption.


# In[ ]:


from confluent_kafka import Producer

# Producer configuration
bootstrap_servers = 'localhost:9092'
topic = 'my-topic'

# Create producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Produce messages
for i in range(10):
    message = f"Message {i}"
    producer.produce(topic, value=message.encode('utf-8'))

# Wait for message delivery
producer.flush()


# In[ ]:


5. Working with Kafka Consumer Groups:
   a) Write a Python program to create a Kafka consumer within a consumer group.
   b) Implement logic to handle messages consumed by different consumers within the same group.
   c) Observe the behavior of consumer group rebalancing when adding or removing consumers.



# In[ ]:


from confluent_kafka import Consumer, KafkaException

# Consumer configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topics = ['my-topic']

# Create consumer instance
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})

# Subscribe to topics
consumer.subscribe(topics)

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        # Process the message
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer
    consumer.close()

