import awswrangler as wr
import pandas as pd
import requests

# importing s3 bucket locations
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "randomuser_api"
csv_path = "personaldata"
path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"


api_url = "https://randomuser.me/api/?results=500"


def get_api_data(api_url):
    """
    fundtion to get data from the API
    Arg: Link to the API
    """
    api_content = requests.get(api_url)
    if api_content.status_code == 200:
        data = api_content.json()
    else:
        print("Error fetching data from API")
        return None
    return data


data = get_api_data(api_url)

male = []
for profile in data["results"]:
    if profile["gender"] == "male":
        male.append(profile["gender"])

male = pd.DataFrame(male, columns=["male_gender"])
# Writing to S3
wr.s3.to_parquet(df=male, path=path + "/male", dataset=True, mode="append")

female = []
for profile in data["results"]:
    if profile["gender"] == "female":
        female.append(profile["gender"])

female = pd.DataFrame(female, columns=["female_gender"])
# Writing to S3
wr.s3.to_parquet(df=female, path=path + "/female", dataset=True, mode="append")

dob_date = []
for profile in data["results"]:
    dob_date.append(profile["dob"]["date"])

dob = pd.DataFrame(dob_date, columns=["date_of_birth"])
# Writing to S3
wr.s3.to_parquet(df=dob, path=path + "/dob", dataset=True, mode="append")


full_names = []
for profile in data["results"]:
    full_names.append(profile["name"]["first"] + " " + profile["name"]["last"])
name = pd.DataFrame(full_names, columns=["full_name"])
# Writing to S3
wr.s3.to_parquet(df=name, path=path + "/fullName", dataset=True, mode="append")
