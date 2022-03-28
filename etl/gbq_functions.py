# A list of funtions for extracting and loading data to/from Google BigQuery
from google.cloud import bigquery

import logging
import nltk

import sys,os
sys.path.append('./') #put other folders on path
if os.getcwd() == "/app/gdlive-explorer":  #If on streamlit cloud, get client via streamlit secrets  
    from streamlit_app.streamlit_cloud_client import get_stcloud_client
    client = get_stcloud_client()
else: #if on local machine or GCP, get it from the local/GCP environment
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "gcp_key.json" #Needed on local machine
    client = bigquery.Client()

def load_recipient(payload):
    """
    Uploads a payload of recipient data to Google BigQuery.

    Parameters
    ----------
    payload : json
        A json payload containing recipient data. 

    """

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
    """
    Uploads a payload of response data to Googel BigQuery.

    Parameters
    ----------
    payload : json
        A json payload containing response data. 

    """

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
    """
    Uploads a payload containing the gender table to Google BigQuery, deletes the old table, and then renames the new data table to replace the old one.

    Parameters
    ----------
    payload : json
        A json payload containing the table the name and the data from the gender classification API. 

    """
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


def get_complete_rids():
    """
    Exracts the recipient_id column from the recipients table on Google BigQuery and returns it as a pandas dataframe.

    Returns
    -------
    completed recipients_ids: Pandas DataFrame
        Recipient IDs of completed profiles in a Pandas DataFrame format.

    """
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
    """
    Exracts the survey_id column from the responses table on Google BigQuery and returns it as a pandas dataframe.

    Returns
    -------
    completed survey_ids: Pandas DataFrame
        IDs of completed surveys in a Pandas DataFrame format.

    """
    query = """
    SELECT DISTINCT CONCAT(responses.recipient_id,"_",responses.payment) AS survey_id
    FROM gdliveproject.tests.responses
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    return job.result().to_dataframe()

def get_names():
    """
    Exracts the distinct values from the name column from the recipient table on Google BigQuery and returns it as a pandas dataframe.

    Returns
    -------
    names: Pandas DataFrame
        Distinct names in a Pandas DataFrame format.

    """
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
    """
    Submits a query request to Google BigQuery that runs a window funtion selecting only the most recently updated row per recipient id.
    The resulting view is then materialized, the old recipient table deleted, and the new table renamed to replace the old one.

    """
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
    """
    Creates the recipient and responses tables, if the do not exist.

    """
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
    """
    Submits a query to Googel BigQuery that joins data from recipient, responses and gender table, aggregating the survey responses by gender, campaign name, payment number, payment amount in USD and the question.
    The resulting view is then loaded in a Pandas DataFrame, after which stopwords are removed using nltk.
    The transformed aggregate table is then loaded to BigQuery, replacing the old table.  

    """
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
    """
    Extracts the aggregate table from Google Big Query and return it as Pandas DataFrame

    Returns
    -------
    Aggregate data: Pandas DataFrame
        A Pandas DataFrame containing the aggregate data table from Google BigQuery

    """
    query = """
    SELECT * 
    FROM `gdliveproject.tests.GDLive_aggregate`
    """
    # labelling our query job
    job = client.query(query)
    
    # results as a dataframe
    df = job.result().to_dataframe()
    return df[df['usdollar'] < 1000]