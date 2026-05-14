from dotenv import load_dotenv
import os
import requests

load_dotenv()

client_id = os.getenv("ORCID_CLIENT_ID")
client_secret = os.getenv("ORCID_CLIENT_SECRET")

response = requests.post(
    "https://orcid.org/oauth/token",
    data={
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    },
)

print(response.status_code)
print(response.json())