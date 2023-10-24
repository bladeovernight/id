import requests

# URL-адрес эндпоинта API
api_url = "https://catfact.ninja/fact"


params = {
  "fact": "string",
  "length": "100"
  
}
response = requests.get(api_url, params)
k=response.json()

print(response.json())