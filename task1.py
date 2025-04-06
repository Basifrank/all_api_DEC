import awswrangler as wr
import pandas as pd
import requests

# loading aws S3 bucket locations
raw_s3_bucket = "chigozieobasi"
raw_path_dir = "jobicy_api"
csv_path = "job_roles"
ur = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

# API url
api_url = "https://jobicy.com/api/v2/remote-j\
obs?count=20&geo=usa&industry=marketing&tag=seo"


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


senior_roles = []
manager_roles = []

for job in data['jobs']:
    if "Senior" in job["jobTitle"]:
        senior_roles.append(job["jobTitle"])
    if "Manager" in job["jobTitle"]:
        manager_roles.append(job["jobTitle"])
senior_roles = {
    "Senior Roles": senior_roles
}
manager_roles = {
    "Manager Roles": manager_roles
}


senior = pd.DataFrame(senior_roles)
manager = pd.DataFrame(manager_roles)


# Writing to S3
wr.s3.to_parquet(df=senior, path=ur + "/senior", dataset=True, mode="append")
wr.s3.to_parquet(df=manager, path=ur + "/manager", dataset=True, mode="append")
