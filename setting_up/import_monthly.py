from bs4 import BeautifulSoup
import requests
import subprocess
import sys

url = sys.argv[1] #'https://download.bls.gov/pub/time.series/sm/'
bucket = sys.argv[2] #'monthly-dump'

def listFD(url):
    '''returns all the files on the url provided'''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return([url + node.get('href').split("/")[-1] for node in soup.find_all('a')])

for filey in listFD(url):
    if len(filey.split("/")[-1].split(".")[-1]) > 0:
        filey_out = filey.split("/")[-1]
        print(f"wget -qO- {filey} | sed 's/ //g' - | aws s3 cp - s3://{bucket}/{filey_out}")
        subprocess.call(f"wget -qO- {filey} | sed 's/ //g' - | aws s3 cp - s3://{bucket}/{filey_out}", shell=True)