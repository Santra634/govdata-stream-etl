import requests

# API details
url = "https://api.data.gov.in/resource/8b75d7c2-814b-4eb2-9698-c96d69e5f128"

params = {
    "api-key": "579b464db66ec23bdd000001a476110a3d03412e5e142883b351c97c",
    "format": "json",
    "limit": 10
}

# Send the request
response = requests.get(url, params=params)

# Check and print result
if response.status_code == 200:
    data = response.json()
    # print(data)
    print("Records:")
    for record in data.get("records", []):
        print(record)
else:
    print("Failed to fetch data. Status code:", response.status_code)
