import requests
from gbq_functions import get_names, replace_gender_table
import pandas as pd
import numpy as np
from google.cloud import secretmanager_v1
from math import ceil


def main():
    """
    Creates or updates a gender_table on Google BigQuery by extracting the distinct names from the recipients table and classifying their gender through the Namsor API.

    """
    name_df = get_names()
    payloads = create_api_payloads(name_df)
    new_gender_table = create_gender_table(payloads)
    replace_gender_table(new_gender_table)

def create_gender_table(payloads):
    """
    Takes a list of API payloads containing the names of participants, splits it into batches of max 100 - the batch size limit of the API -, requests the gender from a Gender API, and returns the result as a Pandas DataFrame.

    Parameters
    ----------
    payloads : list
        List containing the distinct names from the recipients table in the database. 

    Returns
    -------
    gender table: Pandas DataFrame
        A Pandas DataFrame containing the new gender table.

    """
    
    client = secretmanager_v1.SecretManagerServiceClient()
    request = secretmanager_v1.AccessSecretVersionRequest(
        name="projects/717891028584/secrets/gender_api_key/versions/2",
    )
    gender_api_key = client.access_secret_version(request=request).payload.data.decode("utf-8")

    N_PARTITIONS = ceil(len(payloads)/100) #Max size per batch is 100 (at least in March 2022)

    split_payloads = np.array_split(payloads, N_PARTITIONS)

    gender_table = pd.DataFrame()
    for payload in split_payloads:
        payload = {"personalNames": list(payload) }
        gender_batch_df = get_gender_from_api(payload,gender_api_key)
        gender_table = pd.concat([gender_table,gender_batch_df], ignore_index=True)


    gender_table = gender_table.rename(columns={"firstName":"name","likelyGender":"gender"})

    return gender_table[["name","gender","genderScale","score","probabilityCalibrated"]]
       

def create_api_payloads(name_df):
    """
    Takes the Pandas DataFrame containing the names from the recipients table and parses them into a list of payloads compatible with the Namsor API.

    Parameters
    ----------
    payloads : list
        List containing the payloads for the Namsor API

    Returns
    -------
    payloads: list
        List containing a Namsor API payload for each name.

    """
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
    """
    Takes a batch of payloads requests the gender and metadata from the Namsor API. Return the data as Pandas DataFrame.

    Parameters
    ----------
    payloads : list
        List containing the payloads for the Namsor API.
    gender_api_key: str
        The API key.

    Returns
    -------
    response: Pandas DataFrame
        The response converted into a Pandas DataFrame

    """

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