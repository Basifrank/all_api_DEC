import awswrangler as wr
import pandas as pd
import requests

# importing s3 bucket locations
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "football_api"
csv_path = "competitions"
write_path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

api_url = "http://api.football-data.org/v4/competitions/"


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


competition_names = []
for names in data["competitions"]:
    competition_names.append(names["name"])

df_competition = pd.DataFrame(competition_names, columns=["competition_name"])

# Writing to S3
wr.s3.to_parquet(df=df_competition, path=write_path, dataset=True, mode="append")
