import pandas as pd
import requests
import json


#Accesssing the API
api_url = "https://jobicy.com/api/v2/remote-jobs?count=20&geo=usa&industry=marketing&tag=seo"


#Getting the data from the API
api_content = requests.get(api_url)
data = api_content.json()

senior_roles = []
manager_roles = []


"""
Checking for the job titles that contain the word "Senior" and "Manager" 
in the jobTitle key and appending them to the respective lists
"""

for job in data['jobs']:
    
    if "Senior" in job["jobTitle"]:
        senior_roles.append(job["jobTitle"])
        
    if "Manager" in job["jobTitle"]:
        manager_roles.append(job["jobTitle"])

print("Senior Roles: ", senior_roles)
print("Manager Roles: ", manager_roles)