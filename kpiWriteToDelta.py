import requests
import json
import aanalytics2 as api2
import requests as req
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StringType, IntegerType, StructType, StructField , FloatType, DoubleType, DecimalType, DateType
from pyspark.sql.functions import col,lit
from delta.tables import *
from pyspark.sql import SparkSession
import pytz
import os
import fsspec
import sys
import time

spark = SparkSession.builder.getOrCreate()
print('Write to Delta table spark session created')

class writeToDelta():
    def __init__(self, raw_df):

        self.api_config = json.loads(open("/dbfs/tmp/config_path/api_config_uhg.json").read())
        self.job_config = json.loads(open("/dbfs/tmp/config_path/job_config.json").read())
        self.cf_config = json.loads(open("/dbfs/tmp/config_path/cf_config.json").read())

        self.databaseName = self.job_config['databaseName']
        self.tableName = self.job_config['tableName']
        self.deltaDataLoc = self.job_config['deltaDataLoc']
        self.loadType = self.job_config['loadType']
        self._get_curr_time = raw_df._get_curr_time

    def write_delta(self, spark, dataFrame):
        print("writing delta to storage account..")
        #         print(self.loadType) #not needed
        try:
            print("inside try")
            if True:
                tablePath = self.deltaDataLoc
                print("starting upsert")
                oldTable = DeltaTable.forPath(spark, tablePath)
                updated_time = self.curr_time()
                print("printing updated_time")
                print(updated_time)
                oldTable.alias("oldData").merge(dataFrame.alias("newData"),
                                                'oldData.timeid = newData.timeid and oldData.metric_id = newData.metric_id and oldData.lob = newData.lob').whenMatchedUpdate(
                    condition="oldData.metric_value <> newData.metric_value",
                    set={"metric_value": col("newData.metric_value"), "update_ts": lit(updated_time),
                         "metric_name": col("newData.metric_name")}).whenNotMatchedInsertAll().execute()
                print("upsert done")

        #             elif (self.loadType.lower() == "append"):
        #                 dataFrame.write.partitionBy("timeid").format("delta").mode("append").option("overwriteSchema", "true").save(self.deltaDataLoc)

        #             elif (self.loadType.lower() == "overwrite"):
        #                 dataFrame.write.partitionBy("timeid").format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.deltaDataLoc)
        except Exception as e:
            print(str(e))

    # def create_view(self, spark):
    #     spark.sql(f"create database if not exists {self.databaseName}")
    #     createString = f"create table if not exists {self.databaseName}.{self.tableName} ( \n"
    #     finalString = createString + f""") using DELTA location "{self.deltaDataLoc}" """
    #     print(finalString)
    #     spark.sql(finalString)
    #     print("view created!")
