from google.oauth2 import service_account
from google.cloud import bigquery
import os
import streamlit as st

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = st.secrets.gcq_key

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
client = bigquery.Client(credentials=credentials)


def load_recipient(payload):

    schema = [
        bigquery.SchemaField("recipient_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("occupation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("completed", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("campaign", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),

    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        #write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    job = client.load_table_from_json(payload, "gdliveproject.tests.recipients",job_config=job_config)
    job.result()
    print("details of recipients loaded")


def load_response(payload):

    schema = [
        bigquery.SchemaField("response_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("recipient_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("payment", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("usdollar", "INTEGER"),
        bigquery.SchemaField("localfx", "INTEGER"),
        bigquery.SchemaField("question", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("response", "STRING", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        #write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    job = client.load_table_from_json(payload, "gdliveproject.tests.responses",job_config=job_config)
    job.result()
    print("responses loaded")

def load_gender_table(payload):
    from google.cloud import bigquery
    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("gender", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("accuracy", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("sample_size", bigquery.enums.SqlTypeNames.INTEGER),

    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        payload, "gdliveproject.tests.gender", job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table("gdliveproject.tests.gender")  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), "gdliveproject.tests.gender"
        )
    )




# Our SQL Query
def get_complete_rids():
    query = """
    SELECT DISTINCT recipient_id 
    FROM `gdliveproject.tests.recipients`
    WHERE completed = True
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    return job.result().to_dataframe()

def get_complete_surveys():
    query = """
    SELECT DISTINCT CONCAT(responses.recipient_id,"_",responses.payment) AS survey_id
    FROM gdliveproject.tests.responses
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    return job.result().to_dataframe()

def get_names():
    query = """
    SELECT DISTINCT name 
    FROM `gdliveproject.tests.recipients`
    ORDER BY name
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    return job.result().to_dataframe()

def delete_old_participant_details(rid_list):
    query = """
    DELETE gdliveproject.tests.recipients
    WHERE recipient_id IN UNNEST(@rid_list) AND EXTRACT(DATE FROM last_updated) != CURRENT_DATE()
    """
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ArrayQueryParameter("rid_list", "INT64", rid_list),
    ])

    job = client.query(query,job_config=job_config)
    job.result()
    print("Old rids deleted")


def get_aggregate_data():
    query = """
    SELECT * 
    FROM `gdliveproject.tests.GDLive_aggregate`
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    return job.result().to_dataframe()
