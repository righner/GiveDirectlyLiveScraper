#The script used to schedule the scraper in the VM. There is no CI for this script. It is just FYI.
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

with DAG(dag_id="monthly_update",
            start_date=datetime(2022,1,31,0,0),
            schedule_interval=timedelta(months=1),
            catchup=False # 00:00 on the last day of the month
) as dag:
    #pull_image = BashOperator(task_id = "pull_image",
    #                            bash_command= "docker pull europe-west1-docker.pkg.dev/gdliveproject/docker/full:latest",
    #                            dag=dag)



    update_db = BashOperator(task_id = "update_db",
                                bash_command= "docker run europe-west1-docker.pkg.dev/gdliveproject/docker/full:latest",
                                dag=dag)

    update_db