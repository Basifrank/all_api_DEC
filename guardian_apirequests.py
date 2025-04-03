import os
import requests
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv, dotenv_values
load_dotenv()



# s3 environment
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "guardian_api"
csv_path = "nigeria_data"
write_path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

# Load environment variables from .env file

baseurl = "https://content.guardianapis.com/search"
api_key = os.getenv("GUARDIAN_API_KEY")
from_date = "2025-03-01"
to_date = "2025-04-03"
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


# Writing to S3
wr.s3.to_parquet(df=dataframe, path=write_path +"/march_to_april03_2025", dataset=True, mode="append")
