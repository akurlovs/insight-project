# these commands need to be run to prep EC2 cluster

# they still come with python 2... come on, it's 2020
sudo amazon-linux-extras install python3

# and with pip version 9 instead of 20
sudo pip3 install --upgrade pip

# let's get some dependencies in
sudo yum install python3-devel postgresql-devel

pip3 install --upgrade setuptools

sudo yum install gcc

# now that we have the dependencies, let's install some packages!
sudo pip3 install pandas
sudo pip3 install psycopg2
sudo pip3 install dash dash-renderer dash-html-components dash-core-components plotly