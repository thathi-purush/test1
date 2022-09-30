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
print('APICall spark session created')

class APICall():
    def __init__(self, job_config_path, api_config_path, cf_creds_path, jobName):
        ## dbutils outside notebook requires this import
        spark = SparkSession.builder.getOrCreate()
        print('spark session created')
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

        temp_config_path = "dbfs:/tmp/config_path/"
        dir = os.listdir("/dbfs/tmp/config_path")
        print("check destination folder")
        if len(dir) != 0:
            print("destination folder exist and not empty, wiping...")
            dbutils.fs.rm("/tmp/config_path/", True)
            dbutils.fs.mkdirs("/tmp/config_path/")
            dbutils.fs.cp(api_config_path, temp_config_path+"api_config_uhg.json")
            dbutils.fs.cp(job_config_path, temp_config_path+"job_config.json")
            dbutils.fs.cp(cf_creds_path, temp_config_path+"cf_config.json")
            print("configure files copied from sys.argv into dbfs destination folder")
        else:
            print("destination folder checked")
            dbutils.fs.cp(api_config_path, temp_config_path+"api_config_uhg.json")
            dbutils.fs.cp(job_config_path, temp_config_path+"job_config.json")
            dbutils.fs.cp(cf_creds_path, temp_config_path+"cf_config.json")


        print("configs copied!")
        time.sleep(30)

        self.api_config = json.loads(open("/dbfs/tmp/config_path/api_config_uhg.json").read())
        self.job_config = json.loads(open("/dbfs/tmp/config_path/job_config.json").read())
        self.cf_config = json.loads(open("/dbfs/tmp/config_path/cf_config.json").read())

        self.requestEndpoint = self.job_config['requestEndpoint']
        self.requestJsonLoc = self.job_config['requestJsonLoc']
        self.lob = self.job_config['lob']

        print("initialising done!")

    def login(self, config_path):
        api2.importConfigFile(config_path)
        login = api2.Login()
        cids = login.getCompanyId()

        uhg_idx = -1
        for i, cid in enumerate(cids):
            if cid['globalCompanyId'] == 'united8'or cid['globalCompanyId'] == "optumr1":
                uhg_idx = i
        if uhg_idx == -1:
            raise Exception("Could not Login to Adobe-UHG using credentials")

        cid_uhg = 'united8'

        self.endpoint_uhg = api2.Analytics(cid_uhg)
        self.token = api2.retrieveToken()

        self.headers_uhg = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token,
            "x-api-key": self.api_config['api_key'],
            "x-proxy-global-company-id": "united8",
            "x-adobe-dma-company": "United Health Group"
        }

        print("Login successful!")

    def request_call(self):

        ## dbutils outside notebook requires this import
        spark = SparkSession.builder.getOrCreate()
        print('spark session created')
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

        dbutils.fs.cp(self.requestJsonLoc, "dbfs:/tmp/"+self.lob+"/body_json.json")
        self.body = json.loads(open("/dbfs/tmp/"+self.lob+"/body_json.json").read()) # test this in dbr
        y = json.dumps(self.body)
        print("making api call")
        res = req.post(self.requestEndpoint, data=y, headers=self.headers_uhg)
        json_object = json.loads(res.text)
        #print(json_object)

        print("API Request successful!")
        return json_object
