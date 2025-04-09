from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.apache.spark.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='spark_pi_operator_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run Spark Pi job using SparkKubernetesOperator',
) as dag:

    submit_spark_pi = SparkKubernetesOperator(
        task_id='submit_spark_pi',
        namespace='spark-jobs',
        application_file="https://raw.githubusercontent.com/kubeflow/spark-operator/master/examples/spark-pi.yaml",
        do_xcom_push=True,
    )

    monitor_spark_pi = SparkKubernetesSensor(
        task_id='monitor_spark_pi',
        namespace='spark-jobs',
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_pi')['metadata']['name'] }}",
        poke_interval=10,
        timeout=600,
    )

    submit_spark_pi >> monitor_spark_pi
