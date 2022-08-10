from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer('rover-zonechange',
                         bootstrap_servers='rover-cluster-kafka-bootstrap:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

last_seen = {}

for msg in consumer:
    data = msg.value
    data['carId'] = "Alpha"

    if data['previousZoneId']:
        print(f"Rover {data['carId']} left {data['previousZoneId']} at {time.time()}")
        last_seen[data['carId']] = {
            'last_zone': data['previousZoneId'],
            'timestamp': time.time()
        }
        
    elif data['nextZoneId']:
        print(f"Rover {data['carId']} entered {data['nextZoneId']} at {time.time()}")
        if data['carId'] in last_seen:
            print(f"  - This rover was last seen in {last_seen[data['carId']]['last_zone']} trip took {time.time()-last_seen[data['carId']]['timestamp']}")
        
    else:
        print('Zone update malformed.')