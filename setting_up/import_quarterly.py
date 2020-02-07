from bs4 import BeautifulSoup
import requests
import subprocess
import sys

# THIS SCRIPT GETS FILES FROM HTM PAGE AT url_0
# AND THEN RIPS THEM FROM url_1, WHICH HAS TO BE CUSTOMIZED 
# IF THIS SCRIPT WERE TO BE USED FOR ANOTHER PAGE
# AND GENERATED SHELL SCRIPT TO BE RUN IN THE EMR INSTANCE
# TO IMPORT ALL THE FILES

url_0 = sys.argv[1] #'https://www.bls.gov/cew/downloadable-data-files.htm'
url_1 = sys.argv[2] # https://www.bls.gov/cew/data/files
pattern = sys.argv[3] # qtrly_by_area
bucket = sys.argv[4] #'quarterly-dump'
outfile = sys.argv[5] # command to run in 

def listFD(url):
    '''returns all the files on the url provided'''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return([url + node.get('href').split("/")[-1] for node in soup.find_all('a')])

def dld_files_from_url_1():
    '''iterates over file names stripped from htm in url_0 \
       looks for a pattern specified by the pattern variable  \
       and strips the files based on info in url_1
       then creates a shell script to be run on instance'''
    with open(outfile, "w") as out_handle:
        for filey in listFD(url_0):
            delim = url_0.split(".")[-1]
            if len(filey.split("/")[-1].split(".")[-1]) > 0:
                parse_out_name = filey.split("/")[-1]
                if pattern in parse_out_name  and "sic" not in parse_out_name:
                    file_name = parse_out_name.split(delim)[-1]
                    year = file_name.split("_")[0]
                    full_path = f"{url_1}/{year}/csv/{file_name}"
                    out_handle.write(f"wget -qO- {full_path} | aws s3 cp - s3://{bucket}/{file_name}\n")
                    #subprocess.call(f"wget -qO- {filey} | aws s3 cp - s3://{bucket}/{filey_out}", shell=True)

if __name__ == "__main__":
    dld_files_from_url_1()
