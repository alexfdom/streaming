# Check connection of zookeeper and kafka, and schema registry to visualize the data on the Confluent Control Center
# API + Docke(Kafka + Zookeeper + Schema Registry + Control Center)
# A single message is sent to the Kafka producer
from datetime import datetime

import requests

default_args = {"owner": "alexferdg", "start_date": datetime(2024, 4, 2, 00)}


# fetch data from the API
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]

    return res


# clean up and prepare the data for futher processing
def format_data(res):
    data = {}
    location = res["location"]

    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']} {location['state']} {location['country']}"
    )
    data["postcode"] = location["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data


# stream the formatted data to Kafka
def stream_data():
    import json

    from kafka import KafkaProducer

    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    # Send a single message to the producer
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)

    # Data to the queue
    producer.send("user_created_kafka", json.dumps(res).encode("utf-8"))

    # Look the data on the Control Center to see the data: http://localhost:9021/clusters

if __name__ == "__main__":
    stream_data()
