#The script used to schedule the scraper in the VM. There is no CI for this script. It is just FYI.
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from textwrap import dedent

with DAG(dag_id="monthly_update",
            description='Run the scraper and transformations on last day of the month',
            start_date=datetime(2022,3,31,0,0),
            schedule_interval="00 00 L * *", # 00:00 on the last day of the month
            catchup=False 
) as dag:
    extract_load_htmls = BashOperator(task_id = "extract_load_htmls",
                            bash_command= "make -f etl/extract_load_htmls.make",
                            dag=dag)

    extract_load_htmls.doc_md = dedent(
        """\
    # Extract and Load the html of each profile to be updated or added
    Runs the following operations by running the Makefile extract_load_htmls.make:
    1. Get a list of completed rids
    2. Create a list of profile urls to be updated or added: start_rid=158000,interval=10,finish=220000
    3. Download the profiles (10 at a time using xargs) using wget
    4. Compresses them in a tar.gz file
    5. Uploads the tar.gz to Google Cloud Storage 
    6. Deletes the created files and directories
    """
    )
    
    get_htmls = BashOperator(task_id = "get_htmls",
                            bash_command= "gsutil cp gs://gdliveproject_htmls/`date +%Y%m`.tar.gz `date +%Y%m`.tar.gz",
                            dag=dag)

    update_db = BashOperator(task_id = "update_db",
                                bash_command= "sudo docker run europe-west1-docker.pkg.dev/gdliveproject/docker/full:latest",
                                dag=dag)

    update_db.doc_md = dedent(
        """\
    # Update Database
    Runs the following operations by running the docker container:
    1. Create the tables "recipients" and "responses" in BigQuery, if they do not exisit.
    2. Run the scraper with the arguments: start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True
    3. Deletes duplicate recipient info, keeping only the recent update
    4. Recreates the Gender table
    5. Recreates the aggregate data table

    """
    )
    clean_up = BashOperator(task_id = "clean_up",
                            bash_command= "rm `date +%Y%m`.tar.gz -r html",
                            dag=dag)
    
    extract_load_htmls >> get_htmls >> update_db >> clean_up