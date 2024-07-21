import requests
from datetime import datetime
import time

# URL of the FastAPI application
url = "http://127.0.0.1:8000/state"

# Initial data to be inserted
data = {"floor": 2, "vacant": True}

# Function to insert data and print the response
def insert_data(data):
    response = requests.post(url, json=data)
    try:
        response_json = response.json()
    except requests.exceptions.JSONDecodeError:
        response_json = None
    print(f"{datetime.now()}: Response: {response.status_code}, {response_json}")

# Insert initial data
insert_data(data)

# Time intervals in seconds
time_intervals = [5, 7, 9, 12, 13, 15, 17, 20, 23, 26, 28, 30]

# Insert data at different moments in time
for interval in time_intervals:
    time.sleep(interval)
    data["floor"] += 1
    data["vacant"] = not data["vacant"]
    insert_data(data)
