import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer
import random

from collections import deque

id_queue = deque(maxlen=1000)

def scoring_card():
    return random.random()
    
# 初始化Kafka生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 初始化Kafka消费者
consumer = KafkaConsumer(
    'transaction',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='analyzed_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    # print(1)
    # pass
    record = message.value
    id = record['id']

    if id not in id_queue:
        id_queue.append(id)
        record['score'] = scoring_card()
        
        producer.send('analyszed_trans', value=record)
        # print(record)
    