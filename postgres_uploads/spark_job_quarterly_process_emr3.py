from pyspark.sql import SparkSession
import subprocess
import glob
from zipfile import ZipFile
from datetime import datetime

### THE FILES TO BE USED IE THE GLOBALS
INPUT_ZIP_PREFIX = "_qtrly_by_area.zip"
INPUT_BUCKET = "s3://quarterly-dump/"
INPUT_YEARS = range(2019,2020) # years to copy!
HOME_DIR = "/home/hadoop/"
NAICS_FILE = "/home/hadoop/NAICS_2017.txt"

ZIP_DIR = f"{HOME_DIR}unzipped/"
subprocess.call(f"mkdir {ZIP_DIR}", shell=True)


INPUT_ZIPS = [f"{INPUT_BUCKET}{year}{INPUT_ZIP_PREFIX}" for year in INPUT_YEARS]

### FILE-PROCESSING FUNCTIONS ###
def process_naics(naics_file):
    '''processed naics file
       of industry classification
       and outputs dictionary of superindustries
       and subsidiaries'''
    naics_dict = {}
    naics_dict["supersector"] = {}
    naics_dict["subsector"] = {}

    # KEEP TRAck of duplicate categories
    naics_dict["supersector"]["duplicates"] = set()
    
    check = {}

    with open(naics_file) as naics_handle:
        for liney in naics_handle:
            tliney = liney.strip().split("\t")
            category = tliney[0]
            industry = tliney[1]

            if category[:2] not in check:
                check[category[:2]] = set()

            if industry in check[category[:2]]:
                naics_dict["supersector"]["duplicates"].add(category)
            else:
                check[category[:2]].add(industry)

            if len(category) <= 3 or "-" in category:
                if len(category) == 3:
                    naics_dict["subsector"][category] = industry
                else:                     
                    for cats in category.split("-"):
                        naics_dict["supersector"][cats] = industry
    return(naics_dict)


PROCESS_NAICS = process_naics(NAICS_FILE)


### STATES ###

STATES = {
        'AK': 'ALASKA','AL': 'ALABAMA', 'AR': 'ARKANSAS',
        'AS': 'AMERICAN SAMOA', 'AZ': 'ARIZONA', 'CA': 'CALIFORNIA',
        'CO': 'COLORADO', 'CT': 'CONNECTICUT','DC': 'DISTRICT OF COLUMBIA',
        'DE': 'DELAWARE','FL': 'FLORIDA','GA': 'GEORGIA',
        'GU': 'GUAM', 'HI': 'HAWAII', 'IA': 'IOWA', 'ID': 'IDAHO',
        'IL': 'ILLINOIS','IN': 'INDIANA', 'KS': 'KANSAS',
        'KY': 'KENTUCKY', 'LA': 'LOUISIANA','MA': 'MASSACHUSETTS',
        'MD': 'MARYLAND','ME': 'MAINE','MI': 'MICHIGAN',
        'MN': 'MINNESOTA','MO': 'MISSOURI','MP': 'NORTHERN MARIANA ISLANDS',
        'MS': 'MISSISSIPPI', 'MT': 'MONTANA', 'NA': 'NATIONAL',
        'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA',
        'NE': 'NEBRASKA', 'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY',
        'NM': 'NEW MEXICO','NV': 'NEVADA', 'NY': 'NEW YORK', 'OH': 'OHIO',
        'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA',
        'PR': 'PUERTO RICO','RI': 'RHODE ISLAND','SC': 'SOUTH CAROLINA',
        'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS',
        'UT': 'UTAH', 'VA': 'VIRGINIA', 'VI': 'VIRGIN ISLANDS',
        'VT': 'VERMONT', 'WA': 'WASHINGTON','WI': 'WISCONSIN',
        'WV': 'WEST VIRGINIA', 'WY': 'WYOMING'
}


### START SPARK ###

spark = SparkSession \
    .builder \
    .appName("QuarterlyDumpy") \
    .getOrCreate()


### DATA-PROCESSING FUNCTIONS

def csa_msa_county_split(area_in):
    '''figures out if is csa, msa, or county
    and splits the string accordingly'''
    if "CSA" in area_in or "MSA" in area_in:
        area = area_in #area_in.split(",")[0]
        perspective_state = area_in.split(",")[1].split(" ")[1]
        if '-' not in perspective_state:
            state = STATES[perspective_state]
        else:
            state = STATES[perspective_state.split("-")[0]]
        area_type = area_in.split(",")[1].split(" ")[2]
    elif "Statewide" in area_in:
        area = "Statewide"
        state = area_in.split(" ")[0]
        area_type = "State"
    else:
        if "District of Columbia" in area_in:
            area = "Washington, DC"
            state = "DC"
        else:
            area =  area_in #" ".join(area_in.split(", ")[0].split(" ")[:-1])
            state = area_in.split(", ")[1]
        area_type = "County"
    output = [i.lower() for i in [state, area, area_type]]
    return(tuple(output))


def industry_split(industry_in):
    '''Splits industry...
       for now, extracts name only'''
    ind_split = industry_in.split(" ")
    output = []
    
    naics_code = ind_split[1]

    if ("NAICS" in ind_split and 
        naics_code not in PROCESS_NAICS["supersector"]["duplicates"]):

        industry = " ".join(ind_split[2:])

        if naics_code[:2] in PROCESS_NAICS['supersector']:
            output.append(PROCESS_NAICS['supersector'][naics_code[:2]])
        else:
            output.append("")

        if naics_code[:3] in PROCESS_NAICS['subsector']:
            output.append(PROCESS_NAICS['subsector'][naics_code[:3]])
        else:
            if ((len(naics_code) == 2 or "-" in naics_code) and
                (naics_code[:2] in PROCESS_NAICS['supersector'])):
                output.append(f"{PROCESS_NAICS['supersector'][naics_code[:2]]}")
            else:
                output.append("")

        output.append(industry)
        
    else:
        output = ["", "", ""]
    output = [i.lower() for i in output]
    output = [i.replace("-", " ") for i in output]

    return(tuple(output))


def spark_process_file(input_files):
    '''runs a spark process
       on a csv file'''

    for input_file in input_files:

        if "TOTAL" not in input_file and "combined" not in input_file:

            user_log = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", ",").load(input_file)

            user_log.createOrReplaceTempView("quarterly_data")

            processed_frame = spark.sql('''SELECT year,qtr AS quarter,area_title AS area,industry_title AS industry,
                                (month1_emplvl+month2_emplvl+month3_emplvl)/3000.0 AS employees
                                FROM quarterly_data
                                WHERE own_title="Private" AND
                                disclosure_code IS NULL AND
                                month1_emplvl IS NOT NULL AND
                                month2_emplvl IS NOT NULL AND
                                month3_emplvl IS NOT NULL
                                ''').collect()
            

            processed_data = [csa_msa_county_split(i.area)+industry_split(i.industry)+
                                  tuple([datetime.strptime(f"0{j[1]*3-1}/15/{j[0]}", '%m/%d/%Y') if i.quarter < 4 else 
                                        datetime.strptime(f"{j[1]*3-1}/15/{j[0]}", '%m/%d/%Y')
                                         for j in zip([i.year], [i.quarter])])+
                                  tuple([round(i.employees, 3)])
                                  for i in processed_frame if industry_split(i.industry).count("") == 0 ]

            if len(processed_data) > 0:

                new_header = ["state", "area", "area_type", 
                              "supersector", "subsector",
                              "industry", "date", "value"]

                rdd_monthly = spark.sparkContext.parallelize(processed_data)
                df_monthly = rdd_monthly.toDF(new_header)

                df_monthly.write.jdbc(url='jdbc:postgresql://postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com:5432/postgres',
                                    table='quarterly',
                                    mode='append',
                                    properties={'user':'postgres','password':'mypassword',
                                                'driver': 'org.postgresql.Driver'})


def unzip_and_process():
    '''copies zip files from bucket
       unzips them and feeds
       the unzipped csvs into spark'''
    for zippy in INPUT_ZIPS:
        zippy_filename = zippy.split("/")[-1]
        subprocess.call(f"aws s3 cp {zippy} {HOME_DIR}", shell=True)
        zippy_new_path = f"{HOME_DIR}{zippy_filename}"
        zipper = ZipFile(zippy_new_path)
        for i, f in enumerate(zipper.filelist):
            if 'csv' in f.filename:
                if 'TOTAL' in f.filename:
                    f.filename = f'file_{i}_TOTAL.csv'
                elif 'combined' in f.filename:
                    f.filename = f'file_{i}_combined.csv'
                else:
                    f.filename = f'file_{i}.csv'
                zipper.extract(f, path=f"{ZIP_DIR}")
        subprocess.call("hadoop fs -put unzipped", shell=True)
        input_files = glob.glob(f"{HOME_DIR}unzipped/*.csv")

        spark_input_files = [i.replace(f"{HOME_DIR}", "") for i in input_files]

        spark_process_file(spark_input_files)

        subprocess.call("hadoop fs -rm -r unzipped", shell=True)
        subprocess.call(f"rm -r {HOME_DIR}unzipped", shell=True)
        subprocess.call(f"rm -r {zippy_new_path}", shell=True)
        

        
if  __name__ == "__main__":
    unzip_and_process()
