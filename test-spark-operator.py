from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

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

    submit_spark_pi = SparkKubernetesOperator(
        task_id='submit_spark_pi',
        namespace='spark-jobs',  # Đảm bảo namespace đã được tạo trên Kubernetes
        application_file='https://raw.githubusercontent.com/kubeflow/spark-operator/master/examples/spark-pi.yaml',  # Đường dẫn đến file cấu hình của Spark job
        do_xcom_push=True,  # Kết quả từ job Spark có thể được gửi qua XCom
    )

    submit_spark_pi
