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

checkpoint_dict = {
    "moon-checkpoint-a": "Checkpoint A - Dallas",
    "moon-checkpoint-b": "Checkpoint B - Washington",
    "moon-checkpoint-c": "Checkpoint C - New York",
    "moon-checkpoint-d": "Checkpoint D - Raleigh",
    "moon-checkpoint-e": "Checkpoint E - New Orleans",
    "moon-checkpoint-f": "Checkpoint F - Las Vegas",
    "moon-checkpoint-g": "Checkpoint G - San Francisco",
    "moon-checkpoint-h": "Checkpoint H - Los Angeles",
    "moon-outpost-1": "Outpost 1 - Tijuana", 
    "moon-outpost-2": "Outpost 2 - Sydney",
    "moon-outpost-3": "Outpost 3 - Krakow",
    "moon-outpost-4": "Outpost 4 - Warsaw",
    "moon-outpost-5": "Outpost 5 - Prague",
    "moon-outpost-6": "Outpost 6 - Brno",
    "moon-outpost-7": "Outpost 7 - London"
}

for msg in consumer:
    data = msg.value

    if data['previousZoneId']:
        if data['carId'] in rover_dict:
            car_name = rover_dict[data['carId']]
        if data['previousZoneId'] in checkpoint_dict:
            zone_name = checkpoint_dict[data['previousZoneId']]
        print(f"Rover {car_name} left {zone_name} at {time.time()}")
        last_seen[data['carId']] = {
            'last_zone': zone_name,
            'timestamp': time.time()
        }
        
    elif data['nextZoneId']:
        if data['carId'] in rover_dict:
            car_name = rover_dict[data['carId']]
        if data['nextZoneId'] in checkpoint_dict:
            zone_name = checkpoint_dict[data['nextZoneId']]
        print(f"Rover {car_name} entered {zone_name} at {time.time()}")
        if data['carId'] in last_seen:
            print(f"  - This rover was last seen in {last_seen[data['carId']]['last_zone']} trip took {time.time()-last_seen[data['carId']]['timestamp']}")
        
    else:
        print('Zone update malformed.')