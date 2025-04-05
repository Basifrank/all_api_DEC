import os
import requests
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
load_dotenv()


# s3 environment
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "guardian_api"
csv_path = "nigeria_data"
path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

# Load environment variables from .env file

url = "https://content.guardianapis.com/search"
key = os.getenv("GUARDIAN_API_KEY")
start = "2025-03-01"
end = "2025-04-03"
nation = "Nigeria"


def main_request(baseurl, from_date, to_date, x, country, api_key):

    r = requests.get("{}?from-date={}&to-date={}&page={}&q={}\
                     &api-key={}".format(url, start, end, x, nation, key))

    return r.json()


data = main_request(url, start, end, 1, nation, key)


def get_pages(data_page):
    return data_page["response"]["pages"]


def parse_json(data_response):
    charlist = []
    for item in data_response["response"]["results"]:
        char = {
            "publishedDate": item["webPublicationDate"],
            "sectionName": item["sectionName"],
            "webTitle": item["webTitle"],
            "webUrl": item["webUrl"],
            }
        charlist.append(char)
    return charlist

# print(parse_json(data))


parse_json(main_request(url, start, end, 1, nation, key))


# Saving file in csv format

mainlist = []
for x in range(1, get_pages(data)+1):
    mainlist.extend(parse_json(main_request(url, start, end, 1, nation, key)))

df = pd.DataFrame(mainlist)


# Writing to S3
wr.s3.to_parquet(df=df, path=path + "/data", dataset=True, mode="append")
