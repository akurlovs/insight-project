### CREATE BUCKET still need to test
aws s3api create-bucket \
    --bucket monthly-dump \
    --region us-west-2 \
    --create-bucket-configuration LocationConstraint=us-west-2

# IMPORT MONTHLY DATA
python import_montly.py https://download.bls.gov/pub/time.series/sm/ monthly-dump

# First, need to give your IAM account administratinve and EMR privileges
## RUN THIS TO MAKE SURE THE ROLES HAVE BEEN CREATED FOR IAM USER OR IT WON'T WORK
aws emr create-default-roles

# CREATE A SPARK CLUSTER -- need to test more and figure out how to connect to ssh in a pipeline manner
# run it in the same directory as the json
aws emr create-cluster \
    --name "Spark-cluster-emr" \
    --release-label emr-5.29.0 \
    --applications Name=Spark \
    --use-default-roles \
    --ec2-attributes KeyName=andre-IAM-keypair \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large
#

