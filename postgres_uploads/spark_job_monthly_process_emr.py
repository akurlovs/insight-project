# THIS SCRIPT PROCESSES MONTHLY BLS DATA FROM THE BUCKET
# AND UPLOADS IT TO POSTGRES

from pyspark.sql import SparkSession
import subprocess

### THE FILES TO BE USED, IE THE GLOBALS

INPUT_BUCKET = "s3://monthly-dump/"

HOME_DIR = "/home/hadoop/"

INPUT_FILE = "sm.data.0.Current"

### DICTIONARY THAT DEFINES HOW TO SPLIT THE HUGE STRING

SPLIT_DICT = {"seasonal": {"beg": 2, "end": 3},
              "state" : {"beg" : 3, "end" : 5},
              "area" : {"beg": 5, "end": 10},
              "supersector" : {"beg": 10, "end": 12},
              "industry" : {"beg" : 10, "end": 18},
              "data_type" : {"beg" : 18, "end" : 20}
             }
             
#### FUNCTIONS

def dictionarify(filey):
    '''creates key-value pairs
       based on a simple two-column file
       with a header present'''
    out_dict = {}
    with open(filey) as keyval_f:
        next(keyval_f) # skip header
        for liney in keyval_f:
            ln_tab = liney.rstrip().split("\t")
            out_dict[ln_tab[0]] = ln_tab[1].lower()
    return(out_dict)


def build_master_dict(*fileys):
    '''based on files in master dict
       build a dictinary of key-values'''
    master_dict = {}
    for filey in fileys:
        subprocess.call(f"aws s3 cp {INPUT_BUCKET}{filey} {HOME_DIR}", shell=True)
        key_to_add = filey.split(".")[-1] # can change, but appear consistent
        master_dict[key_to_add] = dictionarify(HOME_DIR+filey)
    return(master_dict)


#### GET KEY-VALUE DICTS INTO A MASTER_DICT GLOBAL

MASTER_DICT = build_master_dict("sm.seasonal",
                                "sm.state",
                                "sm.area",
                                "sm.supersector",
                                "sm.industry",
                                "sm.data_type")

### PARSE STRING BASED ON MONHTLY DICT

def parse_monthly_string(monthly_string, *order_keys):
    '''this function breaks down
       string description of fields
       in the montly BLS data
       according to the defined_dict
       and in the order specified by order_keys'''
    prcsd_monthy_str = [MASTER_DICT[key][monthly_string[SPLIT_DICT[key]["beg"]:SPLIT_DICT[key]["end"]]] 
                        for key in order_keys]
    return(tuple(prcsd_monthy_str))

#### START SPARK

spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath","/home/hadoop/postgresql-42.2.9.jar") \
    .appName("MonthlyDumpy") \
    .getOrCreate()

user_log = spark.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .option("delimiter", "\t").load(INPUT_BUCKET+INPUT_FILE)

user_log.printSchema()

user_log.createOrReplaceTempView("monthly_data")

processed_data = [parse_monthly_string(i.series_id, "state", 
                                      "area", "supersector",
                                      "industry", "data_type")+tuple(["MSA", int(f"{i.year}{i.period[1:]}"), 
                                                                 round(float(i.value), 3)])
                                       for i in spark.sql('''SELECT series_id,year,period,value
                                                            FROM monthly_data 
                                                            WHERE footnote_codes IS NULL
                                                            AND period != 'M13'
                                                          ''').collect()]

new_header = ["state", "area", "supersector", "industry",
              "datatype", "area_type", "date", "value"]

rdd_monthly = spark.sparkContext.parallelize(processed_data)
df_monthly = rdd_monthly.toDF(new_header)

df_monthly.printSchema()

df_monthly.createOrReplaceTempView("processed_monthly_data")

df_monthly.write.jdbc(url='jdbc:postgresql://postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com:5432/postgres',
                    table='monthly',
                    mode='overwrite',
                    properties={'user':'postgres','password':'my_password',
                                'driver': 'org.postgresql.Driver'})

# VERIFY UPLOAD
jdbcDF = spark.read.jdbc(url='jdbc:postgresql://postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com:5432/postgres',
                         table='monthly',
                         properties={'user':'postgres','password':'my_password',
                                'driver': 'org.postgresql.Driver'})


jdbcDF.createOrReplaceTempView("monthly_postgres")

spark.sql('''
          SELECT * FROM monthly_postgres \
          LIMIT 8''').show()

