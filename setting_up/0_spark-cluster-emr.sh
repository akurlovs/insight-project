### CREATE BUCKET
aws s3api create-bucket \
    --bucket monthly-dump \
    --region us-west-2 \
    --create-bucket-configuration LocationConstraint=us-west-2

# IMPORT MONTHLY DATA
python /home/kurlovs/Dropbox/andre/insight/labor_project/import_monthly.py https://download.bls.gov/pub/time.series/sm/ monthly-dump

# First, need to give your IAM account administratinve and EMR privileges
## RUN THIS TO MAKE SURE THE ROLES HAVE BEEN CREATED FOR IAM USER OR IT WON'T WORK
aws emr create-default-roles

# CREATE A SPARK CLUSTER
aws emr create-cluster \
    --name "Spark-cluster-emr" \
    --release-label emr-5.29.0 \
    --applications Name=Spark \
    --use-default-roles \
    --ec2-attributes KeyName=andre-IAM-keypair \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large

# IMPORT QUARTERLY DATA - THIS SHOULD BE LESS CLUNKY
# I SHOULD EITHER MAKE THE PYTHON FILE PYTHON 2 -COMPATIBLE
# OR ALTERNATIVELY, BOOTSTRAP EMR TO DEFAULT TO 3

# generate bash scripts to import and unzip files
python /home/kurlovs/Dropbox/andre/insight/labor_project/import_quarterly.py \
       https://www.bls.gov/cew/downloadable-data-files.htm \
       https://www.bls.gov/cew/data/files qtrly_by_area \
       quarterly-dump \
       /home/kurlovs/Dropbox/andre/insight/labor_project/import_quarterly.sh

# ssh to cluster
ssh -i /home/kurlovs/Dropbox/andre/insight/amazon/andre-IAM-keypair.pem hadoop@ec2-34-222-156-167.us-west-2.compute.amazonaws.com
    
# copy the import bash script to cluster
aws s3 cp /home/kurlovs/Dropbox/andre/insight/labor_project/import_quarterly.sh s3://quarterly-dump/

# on emr cluster, run this to import quarterly files
aws s3 cp s3://quarterly-dump/import_quarterly.sh .
bash import_quarterly.sh

# run the unzip scrips
