"""
* @description $ 通过kafka发送测试数据

* @author admin $ qjy20472

* @updateTime $ 上午10:41$ $
"""


import time
from kafka import KafkaProducer

# 创建KafkaProducer实例
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=1)

# 发送消息到主题 "my-topic" 中
topic_name = 'czdf-gc-test-2'
message = '[{"name": "macs", "age": 30, "gender": "male", "kv": [{"key": "123", "value": "abc"}, ' \
          '{"key": "456", "value": "efg"}]}, {"name": "lynn", "age": 30, "gender": "female", "kv": ' \
          '[{"key": "123", "value": "abc"}]},{"name": "macs", "age": 30, "gender": "male", "kv": ' \
          '[{"key": "123", "value": "abc"}, {"key": "456", "value": "efg"}]}, ' \
          '{"name": "lynn", "age": 30, "gender": "female", "kv": [{"key": "123", "value": "abc"}]}]'

# message = '[{"xingming": "macs", "age": 30, "gender": "male"}, ' \
#           '{"xingming": "lynn", "age": 30, "gender": "female"}, ' \
#           '{"xingming": "macs", "age": 30, "gender": "male"}, ' \
#           '{"xingming": "lynn", "age": 30, "gender": "female"}]'

print(message)

while True:
    producer.send(topic_name, message.encode('utf-8'))
    print(message)
    print(time.time())
    time.sleep(1)

# 关闭KafkaProducer实例
producer.close()
