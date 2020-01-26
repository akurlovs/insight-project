from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath","/home/hadoop/postgresql-42.2.9.jar") \
    .appName("MonthlyDumpy") \
    .getOrCreate()


input_bucket = 's3://monthly-dump'
input_file = '/sm.state'
input_path = input_bucket + input_file
#
user_log = spark.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .option("delimiter", "\t").load(input_path)


user_log.createOrReplaceTempView("log_table")
#

print("##################################################################################")
print("#######$$$$$$$$$$$$$$$$$#### SCHEMA TEST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
print("##################################################################################")

user_log.printSchema()

print("##################################################################################")
print("#######$$$$$$$$ WRITE TO A DATABASE AND CHECK SUCCESS $$$$$$$$$$$$$$$$$$$$$$$$$$$$")
print("##################################################################################")

user_log.write.jdbc(url='jdbc:postgresql://postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com:5432/postgres',
                    table='log_table',
                    mode='overwrite',
                    properties={'user':'postgres','password':'mypassword',
                                'driver': 'org.postgresql.Driver'})



jdbcDF = spark.read.jdbc(url='jdbc:postgresql://postgres.cm6tnfe8mefq.us-west-2.rds.amazonaws.com:5432/postgres',
                         table='log_table',
                         properties={'user':'postgres','password':'mypassword',
                                'driver': 'org.postgresql.Driver'})


jdbcDF.createOrReplaceTempView("test_table")

spark.sql('''
          SELECT * FROM test_table \
          LIMIT 8''').show()


print("##################################################################################")
print("#######$$$$$$$$$$$$$$$$$####$$$$$$$ DONE $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$####")
print("##################################################################################")

