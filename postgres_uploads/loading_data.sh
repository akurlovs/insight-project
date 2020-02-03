# loading monthly data
spark-submit --master yarn --deploy-mode client --jars /home/hadoop/postgresql-42.2.9.jar spark_job_monthly_process_emr.py --driver-memory 15G

# loading quarterly data
