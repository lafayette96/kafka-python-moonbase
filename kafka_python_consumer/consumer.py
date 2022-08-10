from kafka import KafkaConsumer
import json
import time
import random

consumer = KafkaConsumer('rover-zonechange',
                         bootstrap_servers='rover-cluster-kafka-bootstrap:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

last_seen = {}

rover_dict ={
    "1303466b-c055-4494-9755-3cf73525fb6f": "Alpha",
    "2c08c081-7cfd-41fd-bfac-c50e4f2a3482": "Beta",
    "8dff909f-6834-4568-9909-257633578179": "Gamma",
    "c90f2c08-ef35-47af-8c88-23a4ee2df223": "Delta",
    "a2025596-a989-4d34-ae65-ac844975cbea": "Pikachu",
    "823f8cdb-d849-40d9-b6a1-6b4f37b38a42": "Ash",
    "3cfef67c-886d-434a-809b-b8e4df8bb68e": "Misty",
    "31815dee-ea88-419c-95c0-69694542cc1e": "Brock",
    "fb1668b3-3725-496d-859e-c31e7e9725d9": "Bravo",
    "a141df52-f80a-4a48-9773-cb4fdd67f8eb": "Charlie"
}

for msg in consumer:
    data = msg.value

    if data['previousZoneId']:
        if data['carId'] in rover_dict:
            car_name = rover_dict[data['carId']]
        print(f"Rover {car_name} left {data['previousZoneId']} at {time.time()}")
        last_seen[data['carId']] = {
            'last_zone': data['previousZoneId'],
            'timestamp': time.time()
        }
        
    elif data['nextZoneId']:
        if data['carId'] in rover_dict:
            car_name = rover_dict[data['carId']]
        print(f"Rover {car_name} entered {data['nextZoneId']} at {time.time()}")
        if data['carId'] in last_seen:
            print(f"  - This rover was last seen in {last_seen[data['carId']]['last_zone']} trip took {time.time()-last_seen[data['carId']]['timestamp']}")
        
    else:
        print('Zone update malformed.')