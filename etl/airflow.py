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
    update_db = BashOperator(task_id = "update_db",
                                bash_command= "sudo docker run europe-west1-docker.pkg.dev/gdliveproject/docker/full:latest",
                                dag=dag)

    update_db.doc_md = dedent(
        """\
    # Update Database
    Runs the following operations by running the docker container:
    1. Create the tables "recipients" and "responses", if they do not exisit.
    2. Run the scraper with the arguments: start_rid=158000,interval=10,number_batches=62,batch_size=100,refresh_gender=True
    3. Deletes duplicate recipient info, keeping only the recent update
    4. Recreates the Gender table
    5. Recreates the aggregate data table

    """
    )
    update_db