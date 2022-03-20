import requests
from gbq_functions import get_names, replace_gender_table
import pandas as pd
import numpy as np
from google.cloud import secretmanager_v1
from math import ceil


def main():
    name_df = get_names()
    payloads = create_api_payloads(name_df)
    new_gender_table = create_gender_table(payloads)
    replace_gender_table(new_gender_table)

def create_gender_table(payloads):
    N_PARTITIONS = ceil(len(payloads)/100) #Max size per batch is 100 (at least in March 2022)
    split_payloads = np.array_split(payloads, N_PARTITIONS)
    
    client = secretmanager_v1.SecretManagerServiceClient()
    request = secretmanager_v1.AccessSecretVersionRequest(
        name="projects/717891028584/secrets/gender_api_key/versions/2",
    )
    gender_api_key = client.access_secret_version(request=request).payload.data.decode("utf-8")
    
    gender_table = pd.DataFrame()
    for payload in split_payloads:
        payload = {"personalNames": list(payload) }
        gender_batch_df = get_gender_from_api(payload,gender_api_key)
        gender_table = pd.concat([gender_table,gender_batch_df], ignore_index=True)

    
    gender_table = gender_table.rename(columns={"firstName":"name","likelyGender":"gender"})

    return gender_table[["name","gender","genderScale","score","probabilityCalibrated"]]
       

def create_api_payloads(name_df):
    payload = []
    for name in name_df['name']:
        load = [{
            "id": name,
            "firstName": name,
            "lastName": ""
        }]
        payload.append(*load)
    return payload

def get_gender_from_api(payload,gender_api_key):

    url = "https://v2.namsor.com/NamSorAPIv2/api2/json/genderBatch"

    headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-API-KEY": gender_api_key
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    return pd.DataFrame(eval(response.text)["personalNames"])





if __name__=='__main__':
    main()