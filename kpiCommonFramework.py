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
print('Common Framework spark session created')

class commonFramework():
    def __init__(self):

        self.api_config = json.loads(open("/dbfs/tmp/config_path/api_config_uhg.json").read())
        self.job_config = json.loads(open("/dbfs/tmp/config_path/job_config.json").read())
        self.cf_config = json.loads(open("/dbfs/tmp/config_path/cf_config.json").read())

        print("getting job details from json")
        self.job_details = self.job_config['job_details']
        self.jobName = self.job_details['jobname']
        self.subjobName = self.job_details['subjobname']

        #self.jobName = jobName
        #self.subjobName = jobName

        self.application = self.job_details['application']
        self.product = self.job_details['product']
        self.runType = self.job_details['runType']
        self.notificationEmail = self.job_details['notificationEmail']
        self.env = self.job_details['env']
        self.subscription = self.job_details['subscription']


        print("getting credentials for common framework")
        self.commonFrameworkClientID = self.cf_config['client-id']
        self.commonFrameworkClientSecret = self.cf_config["client-secret"]
        self.commonFrameworkClientScope = self.cf_config["client-scope"]
        self.jobTrackingKey = self.cf_config["jobtracking-subscription-key"]
        self.ApimKey = self.cf_config["apim-subscription-key"]
        self.tokenEndpoint = self.cf_config["token-endpoint"]
        self.notificationEndpoint = self.cf_config["notification-endpoint"]
        self.jobFailEndpoint = self.cf_config["job-fail-endpoint"]
        self.jobEndEndpoint = self.cf_config["job-end-endpoint"]
        self.jobStartEndpoint = self.cf_config["job-start-endpoint"]

    def generateToken(self):

        headers_token_generation = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        token_generation_data = {
            "grant_type": "client_credentials",
            "scope": self.commonFrameworkClientScope,
            "client_id": self.commonFrameworkClientID,
            "client_secret": self.commonFrameworkClientSecret
        }

        res = req.post(self.tokenEndpoint, data=token_generation_data, headers=headers_token_generation)
        response = res.json()
        token_value = response["access_token"]
        OAuth_Token = "Bearer " + token_value
        return OAuth_Token


    def jobStart(self, OAuth_Token):
        headers_job_start = {
            "Content-type": "application/json",
            "Digicom-Jobtracking-Subscription-Key": self.jobTrackingKey,
            "Authorization": OAuth_Token
        }

        job_start_data = {
            "jobname": self.jobName,
            "subjobname": self.subjobName,
            "application": self.application,
            "product": self.product,
            "prereccount": "0",
            "loadtype": "Incr"
        }

        job_start_data_json = json.dumps(job_start_data)

        res = req.post(self.jobStartEndpoint, data=job_start_data_json,
                       headers=headers_job_start)
        response = res.json()
        print("job start reposnse", response)

        self.jobNotification(OAuth_Token, response['jobStatus'] , response['errorMsg'])
        return response


    def jobEnd(self, OAuth_Token):
        headers_job_end = {
            "Content-type": "application/json",
            "Digicom-Jobtracking-Subscription-Key": self.jobTrackingKey,
            "Authorization": OAuth_Token
        }

        job_end_data = {
            "jobname": self.jobName,
            "subjobname": self.subjobName,
            "extractts": "",
            "postreccount": "0",
            "markmainjobcompleted": "Y"
        }

        job_end_data_json = json.dumps(job_end_data)

        res = req.post(self.jobEndEndpoint, data=job_end_data_json,
                       headers=headers_job_end)
        response = res.json()
        print("job end reposnse", response)

        self.jobNotification(OAuth_Token, response['jobStatus'] , response['errorMsg'])
        return response


    def jobFail(self, OAuth_Token):
        headers_job_fail = {
            "Content-type": "application/json",
            "Digicom-Jobtracking-Subscription-Key": self.jobTrackingKey,
            "Authorization": OAuth_Token
        }

        job_fail_data = {
            "jobname": self.jobName,
            "subjobname": self.subjobName,
            "rejreccount": "",
            "restartstep": self.subjobName,
            "errormsg": "Failed"
        }

        job_fail_data_json = json.dumps(job_fail_data)

        res = req.post(self.jobFailEndpoint, data=job_fail_data_json,
                       headers=headers_job_fail)
        response = res.json()
        print("job fail reposnse", response)

        self.jobNotification(OAuth_Token, response['jobStatus'] , response['errorMsg'])
        return response


    def jobNotification(self, OAuth_Token, status, message):
        headers_adhoc_notification = {
            "Content-type": "application/json",
            "Digicom-Apim-Subscription-Key": self.ApimKey,
            "Authorization": OAuth_Token
        }

        adhoc_notification_data = {
            "jobName": self.jobName,
            "feedName": self.subjobName,
            "status": status,
            "env": self.env,
            "subscription": self.subscription,
            "jobStartTs": "",
            "jobEndTs": "",
            "message": message,
            "frequency": "Monthly",
            "emailIds": [self.notificationEmail]
        }

        adhoc_notification_data_json = json.dumps(adhoc_notification_data)

        res = req.post(self.notificationEndpoint,
                       data=adhoc_notification_data_json, headers=headers_adhoc_notification)
        response = res.json()
        print("notification reposnse", response)

    def publish_metadata(self, metadata_path):
        df=pd.read_csv("/dbfs/FileStore/collibra/kpi_dashoard_metadata.csv") ##metadata_path
        column_list=[]
        column_list=list(df.column)
        for i in column_list:
            col_name=i
            my_comment=df.loc[df['column'] == i, 'metadata'].iloc[0]
            table_name="collibra_poc.dashboard_metrics" ##can be parsed from metadata path
            print(f' table name is {table_name},column name is {col_name}, and comment is {my_comment}')
            spark.sql(f"ALTER TABLE {table_name} CHANGE {col_name} COMMENT '{my_comment}'")
            # if the hive table already exist use the above
            ##spark.sql(f"ALTER TABLE {table_name} Add columns ({col_name} string COMMENT '{my_comment}')")
            # if the hive table not exist then use add columns after the creation

