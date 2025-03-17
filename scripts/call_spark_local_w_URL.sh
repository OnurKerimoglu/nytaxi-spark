export URL="spark://de-zoomcamp.europe-west1-b.c.fresh-gravity-452908-t8.internal:7077"

spark-submit \
--master=${URL} \
spark_sql_local.py \
--input_green=/home/onur/repos/nytaxi-spark/data/pq/green/*/* \
--input_yellow=/home/onur/repos/nytaxi-spark/data/pq/yellow/*/* \
--output=/home/onur/repos/nytaxi-spark/data/report/revenue_monthly_coalesced
