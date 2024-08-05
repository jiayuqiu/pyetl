from kafka import KafkaConsumer
import json
import ast
from pprint import pprint


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'yj_to_sc_diagnose_json',
        group_id='macs',
        auto_offset_reset='latest',
        bootstrap_servers=['10.129.65.137:21268', '10.129.65.141:21269', '10.129.65.138:21270']
    )

    for msg in consumer:
        print("%s:%d:%d: key=%s" % (msg.topic, msg.partition,
                                    msg.offset, msg.key))
        msg_value = msg.value.decode()
        msg_value_json = json.loads(msg_value)
        print(msg_value)
        print(msg_value_json, type(msg_value_json))
        msg_value_dict = ast.literal_eval(msg_value_json)
        # pprint(msg_value_dict)
        print(1)
