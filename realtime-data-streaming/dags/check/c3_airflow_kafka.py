# Check airflow web UI to see the DAGs and the tasks
# API + Docker(Airflow + Postgres + Kafka)
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "alexferdg", "start_date": datetime(2024, 4, 2, 00)}


# fetch data from the API
def get_data():
    import requests

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

    # Running in the Docker container, the hostname is "broker" and the internal ip address is 29092
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    # Data to the queue
    producer.send("single_user", json.dumps(res).encode("utf-8"))


# workflow = series of tasks that need to be executed in a specific order
# DAG = a blueprint of the workflow that defines how the workflow will be executed
with DAG(
    "user_from_api_to_kafka",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:
    streaming_task = PythonOperator(
        task_id="stream_data_from_api", python_callable=stream_data
    )

if __name__ == "__main__":
    stream_data()

# To visualize the execution of the DAG, run the following command: python realtime-data-streaming/dags/check/c2_kafka.py and check the Airflow web UI (http://localhost:8080) and the Kafka Control Center (localhost:9021/clusters)