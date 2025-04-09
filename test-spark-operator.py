import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Hàm tải file YAML từ URL
def download_spark_yaml(**kwargs):
    url = "https://raw.githubusercontent.com/kubeflow/spark-operator/master/examples/spark-pi.yaml"
    response = requests.get(url)
    if response.status_code == 200:
        # Lưu nội dung vào XCom
        kwargs['ti'].xcom_push(key='spark_yaml', value=response.text)
    else:
        raise ValueError(f"Failed to download YAML, status code: {response.status_code}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='spark_pi_with_operator',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Submit Spark Pi job via SparkKubernetesOperator',
) as dag:

    # Task tải file YAML về
    download_yaml = PythonOperator(
        task_id='download_spark_yaml',
        python_callable=download_spark_yaml,
        provide_context=True,
    )

    # Task chạy Spark job sử dụng nội dung YAML từ XCom
    submit_spark_pi = SparkKubernetesOperator(
        task_id='submit_spark_pi',
        namespace='spark-jobs',
        application_file="{{ task_instance.xcom_pull(task_ids='download_spark_yaml', key='spark_yaml') }}",
        do_xcom_push=True,
    )

    download_yaml >> submit_spark_pi
