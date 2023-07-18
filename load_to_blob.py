# Robert Jones
# 2.3.23
# Guided Capstone for SpringBoard
# ...take temp data from spark_parse_from_blob.py and
# ...transform
# ...load back into blob

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

spark = SparkSession.builder.master('local').appName('app').config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstorage.blob.core.windows.net",{azure_key})


class DataLoad:

    def read_temp():
        """Read output (trades and quotes DFs) from spark_parse_blob.py into two DFs """

        trade_common = spark.read.option('inferSchema','true').parquet('combined_trade_and_quote/event_type=T')
        trade = trade_common.select('trade_dt','symbol','exchange','event_tm','event_seq_nb','file_tm','price')

        quote_common = spark.read.option('inferSchema','true').parquet('combined_trade_and_quote/event_type=Q')
        quote = quote_common.select('trade_dt','symbol','exchange','event_tm','event_seq_nb','file_tm')

        return trade,quote

    def drop_dups_keep_latest():

        """Drop duplicates based on trade_dt','symbol','exchange','event_seq_nb and while keeping most recent arrival_tm."""

        quote = DataLoad.read_temp()[1]
        trade = DataLoad.read_temp()[0]

        # Create UniqueID (rank) with orderBy('trade_dt','symbol','exchange','event_seq_nb')
        # ...drop highest rank (duplicate) which will keep most recent exchange
        # ...tested with dummy duplicate on trades.
        trade_window = Window.partitionBy('trade_dt','symbol','exchange','event_seq_nb').orderBy(F.desc('event_tm'))
        trade_corrections = trade.withColumn('rank',F.row_number().over(trade_window)).filter(col('rank') > 1)

        bad_trade_dates = trade_corrections.rdd.map(lambda x:x.trade_dt).collect()
        print(f'Trade dates with corrections {bad_trade_dates}')

        trade = trade.withColumn('rank',F.row_number().over(trade_window)).filter(col('rank') == 1).drop('rank')

        quote_window = Window.partitionBy('trade_dt','symbol','exchange','event_seq_nb').orderBy(F.desc('event_tm'))
        quote_corrections = quote.withColumn('rank',F.row_number().over(quote_window)).filter(col('rank') > 1)
        bad_quote_dates = quote_corrections.rdd.map(lambda x:x.trade_dt).collect()
        print(f'Quote dates with corrections {bad_quote_dates}')

        quote = quote.withColumn('rank',F.row_number().over(quote_window)).filter(col('rank') == 1).drop('rank')

        return trade,quote
    
    def write_to_blob():

        """Write de-duplicated data to Azure Blob"""

        trade = DataLoad.drop_dups_keep_latest()[0]
        quote = DataLoad.drop_dups_keep_latest()[1]

        trade.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/transformed/trades/")
        quote.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/transformed/quotes/")


