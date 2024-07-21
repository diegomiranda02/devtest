import requests
from datetime import datetime
import time

# URL of the FastAPI application
url = "http://127.0.0.1:8000/demand"

# Initial data to be inserted
data = {"floor": 5}

# Function to insert data and print the response
def insert_data(data):
    response = requests.post(url, json=data)
    print(f"{datetime.now()}: Response: {response.status_code}, {response.json()}")

# Insert initial data
insert_data(data)

# Time intervals in seconds
time_intervals = [5, 10, 15]

# Insert data at different moments in time
for interval in time_intervals:
    time.sleep(interval)
    data["floor"] += 1
    insert_data(data)
