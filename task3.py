import pandas as pd
import requests
import json

api_url = "https://randomuser.me/api/?results=500"

api_content = requests.get(api_url)
data = api_content.json()

"""
Extracting male profiles from the data and appending them
"""
male = []
for profile in data["results"]:
  if profile["gender"] == "male":
      male.append(profile)
  
print(male)


"""
Extracting female profiles from the data and appending them
"""
female = []
for profile in data["results"]:
  if profile["gender"] == "female":
      female.append(profile)
  
print(female)



"""
Extracting date of birth date of profiles from the data and appending them
"""
dob_date = []
for profile in data["results"]:
    dob_date.append(profile["dob"]["date"])

print(dob_date)




"""
Extracting full names of profiles from the data and appending them
"""
full_names = []
for profile in data["results"]:
    full_names.append(profile["name"]["first"] + " " + profile["name"]["last"])

print(full_names)