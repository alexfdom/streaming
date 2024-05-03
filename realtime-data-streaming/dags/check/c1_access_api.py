# Check the API response and the data format before sending it to the Kafka queue
# API response: https://randomuser.me/api/
from datetime import datetime
import json
import requests

default_args = {"owner": "alexferdg", "start_date": datetime(2024, 4, 2, 00)}


# fetch data from the API
def get_data():
    res = requests.get("https://randomuser.me/api/")
    # print(res.json())
    res = res.json()
    res = res["results"][0]
    # print(res)
    # print(json.dumps(res, indent=3))

    return res


# stream the formatted data to Kafka
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


def stream_data():
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))


if __name__ == "__main__":
    stream_data()

# To execute this script, run the following command: python realtime-data-streaming/dags/check/c1_access_api.py