# fetch user data from an external web service (API)
# process the data
# and send it to another system for real-time analysis (Kafka)
# We'll use Apache Airflow to automate the process
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import time
import logging


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]

    return res

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

# The JSON object for a single user is generally only a few kilobytes in size
# Given the typical size of the data being handled (a JSON object of a few KB per user), using Airflow XCom to pass this data between tasks wihtin the DAG is reasonalbe and should not pose performance issues.
# XCom is ideal for this level of data size
default_args = {"owner": "alexferdg", "start_date": datetime(2024, 4, 2, 00), "execution_timeout": timedelta(minutes=3)}
@dag( "user_automation",
    default_args=default_args,
    description="DAG for fetching and processing user data from an API and streaming it to Kafka",
    schedule="@daily",
    catchup=False,
)
def taskflow_etl():
    @task
    def stream_data():
        import json
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

        curr_time = time.time()
        while True:
            if time.time() > curr_time + 60:  # 1 minute
                break
            try:
                res = get_data()
                data = format_data(res)

                producer.send("user_created", json.dumps(data).encode("utf-8"))
                producer.flush()
                time.sleep(1)
            except Exception as e:
                logging.error(f"An eror occured: {e}")
                continue
    
    stream_data()

user_data_etl_dag = taskflow_etl()



