# 1. LOG INTO SSH
ssh -i /home/kurlovs/Dropbox/andre/insight/amazon/andre-IAM-keypair.pem hadoop@ec2-54-188-107-73.us-west-2.compute.amazonaws.com

# 2. MAKE SURE PYSPARK RUNS PYTHON3
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh

# 3. COPY STUFF INTO BUCKET (THE PYTHON PROGRAM ACTUALLY RUNNING IT) AND THE POSTGRES JMDB
# DRIVER OBTAINED FROM https://jdbc.postgresql.org/download.html
aws s3 cp /home/kurlovs/Dropbox/andre/insight/labor_project/spark_job_monthly.py s3://monthly-dump/
aws s3 cp /home/kurlovs/Dropbox/andre/insight/labor_project/postgresql-42.2.9.jar s3://monthly-dump/

# 4. IN THE CLUSTER WINDOW, MOVE THE SCRIPT INTO THE CLUSTER SO THAT IT CAN BE RUN:
aws s3 cp s3://monthly-dump/spark_job_monthly.py .
aws s3 cp s3://monthly-dump/postgresql-42.2.9.jar .

# 5. RUN SPARK-CLUSTER IN CLIENT MODE AS IT WON'T RUN OTHERWISE (AT LEAST FROM MY MACHINE)
# TO CONNECT WITH THE DATABASE, MAKE SURE THAT SECURITY GROUPS OF THE DATABASE
# INCLUDE INBOUND PORT 5432 ACCESS BY SECURITY GROUPS OF MASTER EMR NODE
spark-submit --master yarn --deploy-mode client --jars /home/hadoop/postgresql-42.2.9.jar spark_job_monthly.py 
