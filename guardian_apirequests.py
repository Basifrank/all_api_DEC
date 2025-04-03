import os
import requests
import pandas as pd
from dotenv import load_dotenv, dotenv_values
load_dotenv()

# Load environment variables from .env file

baseurl = "https://content.guardianapis.com/search"
api_key = os.getenv("GUARDIAN_API_KEY")
from_date = "2025-03-01"
to_date = "2025-04-01"
country = "Nigeria"



def main_request(baseurl, from_date, to_date, x, country, api_key):
    
    r = requests.get("{}?from-date={}&to-date={}&page={}&q={}&api-key={}".format(baseurl, from_date, to_date, x, country, api_key))

    return r.json() 

data = main_request(baseurl, from_date, to_date, 1, country, api_key)

 
def get_pages(data_page):
    return data_page["response"]["pages"]

def parse_json(data_response):
    charlist = []
    for item in data_response["response"]["results"]:
        char = {
            "publishedDate" : item["webPublicationDate"],
            "sectionName" : item["sectionName"],
            "webTitle" : item["webTitle"],
            "webUrl" : item["webUrl"],
            }
        charlist.append(char)
    return charlist

#print(parse_json(data))

parse_json(main_request(baseurl, from_date, to_date, 1, country, api_key))


#Saving file in csv format

mainlist = []
for x in range(1, get_pages(data)+1):
    mainlist.extend(parse_json(main_request(baseurl, from_date, to_date, 1, country, api_key)))

dataframe = pd.DataFrame(mainlist)

dataframe.to_csv("updated_data_MarApr.csv", index=False)

print(dataframe.head())
