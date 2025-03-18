#!/usr/bin/env python
# imports

import argparse
import os
import pandas as pd
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types

# Read arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output
print(f"""
spark_sql_local called with arguments:
input_green: {input_green}
input_yellow: {input_yellow}
output: {output}
""")

# paths
rootpath = os.path.dirname(os.path.abspath(""))
datapath = os.path.join(rootpath, 'data')
print(f"datapath: {datapath}")

# We can either supply the url of the standalone spark master here, and call the script directly from terminal
# spark = SparkSession.builder \
#     .master("spark://de-zoomcamp.europe-west1-b.c.fresh-gravity-452908-t8.internal:7077") \
#     .appName('sql_local') \
#     .getOrCreate()

# Or we can omit it here, and use spark-submit instead, like:
# spark-submit --master="spark://de-zoomcamp.europe-west1-b.c.fresh-gravity-452908-t8.internal:7077" spark_sql_local.py
spark = SparkSession.builder \
    .appName('sql_local') \
    .getOrCreate()

#df_green = spark.read.parquet(os.path.join(datapath, 'pq', 'green', '*', '*'))
df_green = spark.read.parquet(input_green)
# df_green.show()
# df_green.printSchema()

# df_yellow= spark.read.parquet(os.path.join(datapath, 'pq', 'yellow', '*', '*'))
df_yellow= spark.read.parquet(input_yellow)
# df_yellow.show()
# df_yellow.printSchema()


df_green = df_green \
.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
.withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
.withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
df_yellow.printSchema()

# common columns:
#common_columns = []
# yellow_columns = df_yellow.columns
# for col in df_green.columns:
#     if col in yellow_columns:
#         common_columns.append(col)

common_columns = ['VendorID', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'payment_type', 'congestion_surcharge']

df_green_sel = df_green\
.select(common_columns)\
.withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow\
.select(common_columns)\
.withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# df_green.count()
# df_yellow.count()

df_trips_data.count()

df_trips_data.groupBy('service_type').count().show()


df_trips_data.registerTempTable('trips_data')


query = """
select 
-- Revenue grouping 
PULocationID as revenue_zone,
date_trunc("month", "pickup_datetime") as revenue_month,
service_type, 
-- Revenue calculation 
sum(fare_amount) as revenue_monthly_fare,
sum(extra) as revenue_monthly_extra,
sum(mta_tax) as revenue_monthly_mta_tax,
sum(tip_amount) as revenue_monthly_tip_amount,
sum(tolls_amount) as revenue_monthly_tolls_amount,
sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
sum(total_amount) as revenue_monthly_total_amount,
-- Additional calculations
avg(passenger_count) as avg_monthly_passenger_count,
avg(trip_distance) as avg_monthly_trip_distance
from trips_data
group by 
1,2,3
"""

df_result = spark.sql(query)

df_result.show()

# df_result.coalesce(1).write.parquet(os.path.join(datapath, 'report', 'revenue_monthly_coalesced'), mode='overwrite')
df_result.coalesce(1).write.parquet(output, mode='overwrite')

# spark.sparkContext.stop()

