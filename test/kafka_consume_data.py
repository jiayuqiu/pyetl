from kafka import KafkaConsumer
import traceback
import json

# 创建KafkaConsumer实例
consumer = KafkaConsumer(
    bootstrap_servers=['10.129.65.137:21671', '10.129.65.140:21670', '10.129.65.136:21669'], 
    auto_offset_reset='latest', 
    enable_auto_commit=True
)

# 订阅 Topic 并接收消息
topic_name = 'dispatcher_sdec_data'
consumer.subscribe(topics=[topic_name])

# # Define the UDF for parsing JSON
# def parse_json(json_string):
#     print(1, json_string, type(json_string))
#     if json_string is None:
#         return None
#     try:
#         json_data = json.loads(json_string)
#         return_list = []
#         print(2, json_data, type(json_data))
#         for data_ in json_data:
#             return_data = {}
#             print(3, data_, type(data_))
#             for key in data_.keys():
#                 if key in ['xingming', 'gender', 'age']:
#                     return_data[key] = data_[key]
#             return_list.append(return_data)
#         print(return_list)
#         return return_list
#     except ValueError:
#         traceback.print_exc()
#         return None

data_cnt = 0
data_list = []
for message in consumer:
    msg = message.value.decode('utf8')
    msg_dict = json.loads(msg)
    data_list.append(msg_dict)
    data_cnt += 1
    if data_cnt == 5:
        break

# 关闭KafkaConsumer实例
with open('docs/sdec_msg.json', 'w') as f:
    json.dump(data_list, f)

consumer.close()
