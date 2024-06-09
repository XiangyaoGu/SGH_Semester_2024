import json
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import datetime
from collections import deque

from update_stat_results import update_stat_results


df_received = pd.DataFrame()
buffer_maxlen = 50000
update_speed_seconds = 5
data_buffer = deque(maxlen=buffer_maxlen)
cols = ['id', 'category', 'amt', 'gender', 'city', 'state', 'city_pop',
       'job', 'unix_time', 'score']

def existing_ids(buffer):
    return [o[0] for o in buffer]


def df_from_buffer(data_buffer):

    return pd.DataFrame(data=data_buffer, columns=cols)
# def append_to_buffer(data_buffer, record):
#     # new_row = pd.DataFrame([record])
#     # df = pd.concat([df, new_row], ignore_index=True)
#     return df


# 初始化Kafka消费者
consumer = KafkaConsumer(
    'analyszed_trans',  # 输入主题
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='analyzed_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

last_time = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

for message in consumer:
    # print(1)
    record = message.value

    id = record['id']

    if id not in existing_ids(data_buffer):
    
        row = []
        # print(record)
        for col in cols:
            row.append(record[col])
        
        data_buffer.append(row)
        
        now_time = datetime.datetime.now(tz=datetime.timezone.utc)
        if now_time >= last_time + datetime.timedelta(seconds=update_speed_seconds):
            
            last_time = now_time

            df = df_from_buffer(data_buffer)
            # print("before update")
            print("row number: ", df.shape[0])
            # print(df.tail(3))
            # print("after update")
            # update_stat_results(df)
            update_stat_results(df)
            
        # break
        

    # df_received = append_to_dataframe(df_received, record)  # 将记录转换为DataFrame
    # print(df_received)  # 打印DataFrame，验证转换结果
    
    # producer.send('analyszed_trans', value=record)

# 刷新生产者
# producer.flush()