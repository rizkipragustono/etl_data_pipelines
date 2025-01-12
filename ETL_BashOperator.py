from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'lol',
    'start_date': days_ago(0),
    'email': ['lolrandom@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)as dag:

    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command="""cd /home/project/airflow/dags/finalassignment/
                        tar -xzf tolldata.tgz -C /home/project/airflow/dags/finalassignment/"""
    )

    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command="""
        awk -F',' 'NR == 1 {print $1, $2, $3, $4} NR > 1 {print $1, $2, $3, $4}' \
        /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv
        """,
    )

    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command="""
        awk -F'\t' 'BEGIN {OFS=","} NR == 1 {print $5, $6, $7} NR > 1 {print $5, $6, $7}' \
        /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv
        """,
    )

    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command="""
        awk 'BEGIN {OFS=","} {print substr($0, 59, 3), substr($0, 63, 5)}' \
        /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv
        """,
    )

    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command="""
        paste -d',' /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv
        """,
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command="""
        awk -F, 'BEGIN {OFS=","} NR == 1 {print $0} NR > 1 { $3=toupper($3); print $0 }' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
        """
    )

    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data