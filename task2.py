import pandas as pd
import requests
import json

api_url = "http://api.football-data.org/v4/competitions/"

api_content = requests.get(api_url)
data = api_content.json()

competition_names = []
for names in data["competitions"]:
    competition_names.append(names["name"])

print(competition_names)