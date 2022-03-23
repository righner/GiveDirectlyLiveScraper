from google.cloud import bigquery
import logging
import nltk

import sys
sys.path.append('./')
try:
    client = bigquery.Client()
except Exception as e:
    print(e)
    print("Streamlit Cloud detected: Using Streamlit Cloud access GCP client.")
    from streamlit.streamlit_cloud_client import get_stcloud_client
    client = get_stcloud_client()


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
    try:
        job.result()
        logging.info("details of recipients loaded")
    except:
        logging.error("Error loading this recipient info payload to BigQuery:""\n"+str(payload))



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
        )
    job = client.load_table_from_json(payload, "gdliveproject.tests.responses",job_config=job_config)
    try:
        job.result()
        logging.info("responses loaded")
        return True
    except:
        logging.error("Error loading this response payload to BigQuery:""\n"+str(payload))

def replace_gender_table(payload):
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
        bigquery.SchemaField("genderScale", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("score", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("probabilityCalibrated", bigquery.enums.SqlTypeNames.FLOAT),
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
    logging.info(
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

def delete_old_participant_details():
    query = """
    CREATE TABLE IF NOT EXISTS gdliveproject.tests.updated AS (
        WITH added_row AS (
        SELECT
            recipient_id,
            name,
            age,
            occupation,
            completed,
            country,
            campaign,  
            last_updated,
            ROW_NUMBER() OVER(PARTITION BY recipient_id ORDER BY last_updated DESC) AS row_number
        FROM gdliveproject.tests.recipients
        )
        SELECT
        *
        FROM added_row
        WHERE row_number = 1
        ORDER BY recipient_id 
        );
    DROP TABLE gdliveproject.tests.recipients;
    ALTER TABLE gdliveproject.tests.updated RENAME TO recipients    
        """

    job = client.query(query)
    job.result()
    logging.info("Old rids deleted")


def try_create_recipient_response_tables():
    query = """
    -- recipients table
    CREATE TABLE IF NOT EXISTS gdliveproject.tests.recipients (
        recipient_id INTEGER NOT NULL,
        name STRING NOT NULL,
        age INTEGER,
        occupation STRING,
        completed boolean NOT NULL,
        country STRING NOT NULL,
        campaign STRING NOT NULL,
        last_updated TIMESTAMP NOT NULL );

    -- responses table
    CREATE TABLE IF NOT EXISTS gdliveproject.tests.responses (
        response_id STRING NOT NULL,
        recipient_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        payment INTEGER NOT NULL,
        usdollar INTEGER,
        localfx INTEGER,
        question STRING NOT NULL,
        response STRING NOT NULL,
    );  
            """

    job = client.query(query)
    job.result()
    logging.info("Recipient & response tables created (if not existing)")

def create_aggregate_table():
    query = """SELECT gender.gender as gender, recipients.campaign,responses.payment,responses.usdollar,responses.question, STRING_AGG(responses.response) AS agg_response
    FROM gdliveproject.tests.recipients
    INNER JOIN gdliveproject.tests.gender
    ON recipients.name = gender.name
    INNER JOIN gdliveproject.tests.responses
    ON recipients.recipient_id = responses.recipient_id
    GROUP BY gender,recipients.campaign,responses.payment,responses.usdollar,responses.question;
    """
    job = client.query(query)
    
    # results as a dataframe
    df = job.result().to_dataframe()
    logging.info("Data joined and downloaded")
    nltk.download('stopwords')
    from nltk.corpus import stopwords

    stop_words = stopwords.words('english') + ['money', 'GD','GiveDirectly','Give','Directly', 'first','second','third', 'transfer','transfers','KES','kes','UGX','ugx','biggest', 'hardship']
    
    df['agg_response'] = df['agg_response'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))

    logging.info("Stopwords deleted")
    schema = [
        bigquery.SchemaField("gender", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payment", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("usdollar", "INTEGER",mode = "NULLABLE"),
        bigquery.SchemaField("question", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("agg_response", "STRING", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

    job = client.load_table_from_dataframe(
        df, "gdliveproject.tests.GDLive_aggregate" , job_config=job_config
    )  # Make an API request.
    job.result()

    logging.info("Aggregate data loaded")


def get_aggregate_data():
    query = """
    SELECT * 
    FROM `gdliveproject.tests.GDLive_aggregate`
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    df = job.result().to_dataframe()
    return df[df['usdollar'] < 1000]