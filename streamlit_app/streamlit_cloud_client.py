from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit as st

def get_stcloud_client():
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        print("Streamlit Cloud detected: Using Streamlit Cloud access GCP client.")
        return bigquery.Client(credentials=credentials)
    except:
        pass