from gbq_functions import get_names, load_gender_table
from json import loads
from urllib.request import urlopen
from google.cloud import secretmanager_v1



def create_gender_table():
    client = secretmanager_v1.SecretManagerServiceClient()
    request = secretmanager_v1.AccessSecretVersionRequest(
        name="projects/717891028584/secrets/gender_api_key/versions/1",
    )
    gender_api_key = client.access_secret_version(request=request).payload.data.decode("utf-8")
    table_df = get_names()
    table_df[["gender","accuracy","sample_size"]] = table_df.apply(get_gender,gender_api_key=gender_api_key,axis=1, result_type ='expand')

    load_gender_table(table_df)

def get_gender(row,gender_api_key):
    print(row.name)
    api_url = "https://gender-api.com/get?key=" + gender_api_key + "&name=" + row["name"]
    response = urlopen(api_url)
    decoded = response.read().decode('utf-8')
    data = loads(decoded)
    gender = data["gender"]
    accuracy = data["accuracy"]
    sample = data["samples"]

    return gender, accuracy, sample