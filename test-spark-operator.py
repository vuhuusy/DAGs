from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="spark_pi_example",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    spark_pi = SparkKubernetesOperator(
        task_id='spark_pi_submit',
        namespace='spark-jobs',
        application_file='spark-pi.yaml',  # relative path trong container
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=False
    )
