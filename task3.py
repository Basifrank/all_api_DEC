import pandas as pd
import requests
import awswrangler as wr

# importing s3 bucket locations
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "randomuser_api"
csv_path = "personaldata"
write_path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"


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
      
df_male = pd.DataFrame(male, columns=["male_gender"])
# Writing to S3
wr.s3.to_parquet(df=df_male, path=write_path +"/male_gender", dataset=True, mode="append")

female = []
for profile in data["results"]:
  if profile["gender"] == "female":
      female.append(profile["gender"])
      
df_female = pd.DataFrame(female, columns=["female_gender"])
# Writing to S3
wr.s3.to_parquet(df=df_female, path=write_path +"/female_gender", dataset=True, mode="append")

dob_date = []
for profile in data["results"]:
    dob_date.append(profile["dob"]["date"])

date_of_birth = pd.DataFrame(dob_date, columns=["date_of_birth"])
# Writing to S3
wr.s3.to_parquet(df=date_of_birth, path=write_path +"/date_of_birth", dataset=True, mode="append")


full_names = []
for profile in data["results"]:
    full_names.append(profile["name"]["first"] + " " + profile["name"]["last"])
full_name = pd.DataFrame(full_names, columns=["full_name"])
# Writing to S3
wr.s3.to_parquet(df=full_name, path=write_path +"/full_name", dataset=True, mode="append")
