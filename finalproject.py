
import requests


# position stack
API_KEY = "e1534616c63441ebf4d00f2ed847b6d5"

url = "http://api.positionstack.com/v1/forward"
params = {
    "access_key": API_KEY,
    "query": "England"
}

response = requests.get(url, params=params)
print(response.json())




#weather
API_KEY = "240695f1e2a6874957578b46f2c95ba3"

url = "http://api.positionstack.com/v1/forward"
params = {
    "access_key": API_KEY,
    "query": "England"
}

response = requests.get(url, params=params)
print(response.json())




#crime
import requests

url = "https://data.police.uk/api/crimes-street/all-crime"
params = {
    "lat": 52.629729,
    "lng": -1.131592,
    "date": "2023-01"
}

response = requests.get(url, params=params)
data = response.json()

print(data[:3])  # Preview first three crimes
