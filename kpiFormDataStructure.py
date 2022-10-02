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
print('Form Data Structure spark session created')

class formDataStructure():
    def __init__(self, api_instance):

        self.api_config = json.loads(open("/dbfs/tmp/config_path/api_config_uhg.json").read())
        self.job_config = json.loads(open("/dbfs/tmp/config_path/job_config.json").read())
        self.cf_config = json.loads(open("/dbfs/tmp/config_path/cf_config.json").read())

        # self.refreshMapping = self.job_config['refreshMapping']
        self.endpoint_uhg = api_instance.endpoint_uhg
        # self.metricSource = self.job_config['metricSource']
        # self.lob = self.job_config['lob']
        # self.headers_uhg = api_instance.headers_uhg
        # self.body=api_instance.body
        # self.timeLevel = self.job_config['timeLevel']
        # self.job_details = self.job_config['job_details']
        # self.jobName = self.job_details['jobname']

    def get_calcmetric_mapping(self):
        calcmetric_df = pd.read_csv("/dbfs/tmp/calcmetrics_mapping.csv")
        print("Calculated Metrics Mapping written successfully!")

        return calcmetric_df

    # def _get_column_name(self, column_id, calcmetric_df):
    #     col_row = calcmetric_df[calcmetric_df['id'] == column_id]
    #
    #     if col_row.shape[0] == 0 or col_row.shape[0] > 1:
    #         return None
    #     if "mobileapp" in self.jobName:
    #         col = col_row.iloc[0]['description']
    #     else:
    #         col = col_row.iloc[0]['name']
    #     return col

    # def _get_date(self, rsid, dimension, date_today, headers):
    #     item_endpoint = "https://analytics.adobe.io/api/united8/reports/topItems?rsid={rsid}&dimension={dimension}&locale=en_US&lookupNoneValues=true&limit=1200&page=0&dateRange=2020-01-01T00%3A00%3A00.000%2F{today}T00%3A00%3A00.000"
    #     res = req.get(item_endpoint.format(dimension=dimension, rsid=rsid, today=date_today), headers=headers)
    #     res_json = json.loads(res.text)
    #     res_rows = res_json['rows']
    #     return res_rows
    #     #print(res_rows)

    def _get_metric_name_segments(self, lob_evar38, evar38Flag, metric_name):

        if (self.metricSource).lower() == "adobe":
            category_type = "Site Activity"
        try:
            splitted = metric_name.split("|")
            channel = splitted[2].strip()
            product = splitted[3].strip()
            metric_name = splitted[4].strip()

            if splitted[5] == '':
                category = "Stand Alone"
            else:
                category = splitted[5].strip()

            lob_metric_name = splitted[1].strip()
            if evar38Flag == "Y":
                lob = lob_evar38.upper()
                return [channel, lob, product, category_type, category, metric_name]
            else:
                if self.lob.lower() == "no filter":
                    lob = "ALL LoB"
                    return [channel, lob, product, category_type, category, metric_name]

                if lob_metric_name == "":
                    lob = "ALL LoB"
                    return [channel, lob, product, category_type, category, metric_name]

                else:
                    lob = lob_metric_name.upper()
                    return [channel, lob, product, category_type, category, metric_name]

        except:
            raise Exception("length of metric name isnt correct!")

    # def _get_report_suite(self, rsid):
    #     item_endpoint = f"https://analytics.adobe.io/api/united8/reportsuites/collections/suites/{rsid}"
    #     res = req.get(item_endpoint, headers = self.headers_uhg)
    #     res_json = json.loads(res.text)
    #     return res_json['name']

    def _get_curr_time(self):
        tz_CHI = pytz.timezone('America/Chicago')
        datetime_CHI = datetime.now(tz_CHI)
        current_time = datetime_CHI.strftime("%Y-%m-%d %H:%M:%S")
        return(current_time)

    # def _check_lob(self, metric_name):
    #     evar38Flag = ""
    #     try:
    #         lob_evar38 = self.body['globalFilters'][0]['segmentDefinition']['container']['pred']['list'][0]
    #         evar38Flag = "Y"
    #         return (self._get_metric_name_segments(lob_evar38, evar38Flag, metric_name))
    #     except:
    #         lob_evar38 = ""
    #         evar38Flag = "N"
    #         arr = self._get_metric_name_segments(lob_evar38, evar38Flag, metric_name)
    #         return (arr)
    def get_metric_name_desc_segments(self, check_lob, metric_desc):
        category_type = "Site Activity"
        try:
            splitted = re.split('[\[\|\]]', metric_desc)
            # print(splitted)
            channel = splitted[3].strip()
            product = splitted[4].strip()
            metric_name = splitted[5].strip()
            if len(splitted) < 7:
                category = "Stand Alone"
            else:
                if splitted[6] == '':
                    category = "Stand Alone"
                else:
                    category = splitted[6].strip()

            lob_metric_name = splitted[2].strip()
            if check_lob.lower() == "no filter":
                lob = "ALL LoB"
                return [channel, lob, product, category_type, category, metric_name]
            else:
                lob = lob_metric_name.upper()
                return [channel, lob, product, category_type, category, metric_name]
        except:
            raise Exception("length of metric desc isnt correct!")
    def get_metric_name_desc_segments_calcmetric_df(self, check_lob, metric_name, calcmetric_df):

        if (self.metricSource).lower() == "adobe":
            category_type = "Site Activity"
        try:
            match_index_df = df[df['name'] == metric_name]
            if not match_index_df.empty:
                metric_desc = match_index_df['description'].values[0]
            else:
                print('Unable to fecth/find the col_name in calcmetric_df')

            splitted = re.split('[\[\|\]]', metric_desc)
            # print(splitted)
            channel = splitted[3].strip()
            product = splitted[4].strip()
            metric_name = splitted[5].strip()

            if splitted[6] == '':
                category = "Stand Alone"
            else:
                category = splitted[6].strip()
            lob_metric_name = splitted[2].strip()
            if check_lob.lower() == "no filter":
                lob = "ALL LoB"
                return [channel, lob, product, category_type, category, metric_name]
            else:
                lob = lob_metric_name.upper()
                return [channel, lob, product, category_type, category, metric_name]

        except:
            raise Exception("length of metric desc isnt correct!")
    def form_data_structure(self, fpc_raw_data, calcmetric_df, timelvlnm):
        ##main function for data transformation

        print("forming data structure..")
        data = []
        fpc_raw_data = fpc_raw_data.astype(str)
        date_today = datetime.today().strftime('%Y-%m-%d')
        try:
            if '# Date' in fpc_raw_data['col1'][3]:
                year = int(fpc_raw_data['col1'][3][-4:])
                print(year)
        except:
            raise Exception("order of metric filter isn't correct: Unable to fetch year ")
        try:
            rs_name = (fpc_raw_data['col1'][2]).split(': ')[1]
            print(rs_name)
            if rs_name:

                rptst_df = self.endpoint_uhg.getReportSuites()
                rptrsid = rptst_df[rptst_df['name'] == rs_name]
                if not rptrsid.empty:
                    rsid = rptrsid['rsid'].values[0]

                    print("printing rsid")
                    print(rsid)
                else:
                    print(f"unable to fetch rsid with the given rpt suite name: {rs_name}")
                    exit()
            else:
                print("unable to fetch rs_name from the raw csv report")
                exit()
        except:
            raise Exception("report suite or issue")
        check_lob = (fpc_raw_data['col1'][2]).split(': ')[1]
        if ',' in check_lob:
            check_lob = check_lob.split(',')[0]
        else:
            check_lob = 'no filter'
        for ind in fpc_raw_data.index:
            if '#' in fpc_raw_data['col1'][ind] and 'Metrics' in fpc_raw_data['col1'][ind] and 'by' not in \
                    fpc_raw_data['col1'][ind]:
                # Metrics_name = fpc_raw_data['col1'][ind].split('# ')[1]
                FPC_KPI_Metrics = pd.DataFrame(columns=['metrix', 'all_visits_this_year', 'all_visits_last_year'])
                for new_ind in fpc_raw_data.index[ind + 2:]:
                    if '####' in fpc_raw_data['col1'][new_ind]:
                        break
                    if '|' in fpc_raw_data['col1'][new_ind]:
                        record = f"{fpc_raw_data['col1'][new_ind]},{fpc_raw_data['col2'][new_ind]},{fpc_raw_data['col3'][new_ind]}"
                        FPC_KPI_Metrics = FPC_KPI_Metrics.append(
                            {'metrix': fpc_raw_data['col1'][new_ind],
                             'all_visits_this_year': fpc_raw_data['col2'][new_ind],
                             'all_visits_last_year': fpc_raw_data['col3'][new_ind]}, ignore_index=True)
            #                 print(FPC_KPI_Metrics)
            elif '#' in fpc_raw_data['col1'][ind] and 'Metrics' in fpc_raw_data['col1'][ind] and 'month - this' in \
                    fpc_raw_data['col1'][ind] and timelvlnm == 'Month':
                FPC_KPI_Metrics_By_Month_this_year = pd.DataFrame(
                    columns=['metrix', 'all_visits_this_year_january', 'all_visits_this_year_february',
                             'all_visits_this_year_march',
                             'all_visits_this_year_april', 'all_visits_this_year_may', 'all_visits_this_year_june',
                             'all_visits_this_year_july', 'all_visits_this_year_august',
                             'all_visits_this_year_september',
                             'all_visits_this_year_october', 'all_visits_this_year_november',
                             'all_visits_this_year_december'])
                for new_ind in fpc_raw_data.index[ind + 5:]:
                    if '####' in fpc_raw_data['col1'][new_ind]:
                        break
                    if '|' in fpc_raw_data['col1'][new_ind]:
                        record = f"{fpc_raw_data['col1'][new_ind]},{fpc_raw_data['col2'][new_ind]},{fpc_raw_data['col3'][new_ind]},{fpc_raw_data['col4'][new_ind]},{fpc_raw_data['col5'][new_ind]},{fpc_raw_data['col6'][new_ind]},{fpc_raw_data['col7'][new_ind]},{fpc_raw_data['col8'][new_ind]},{fpc_raw_data['col9'][new_ind]},{fpc_raw_data['col10'][new_ind]},{fpc_raw_data['col11'][new_ind]},{fpc_raw_data['col12'][new_ind]},{fpc_raw_data['col13'][new_ind]}"
                        FPC_KPI_Metrics_By_Month_this_year = FPC_KPI_Metrics_By_Month_this_year.append(
                            {'metrix': fpc_raw_data['col1'][new_ind],
                             'all_visits_this_year_january': fpc_raw_data['col2'][new_ind],
                             'all_visits_this_year_february': fpc_raw_data['col3'][new_ind],
                             'all_visits_this_year_march': fpc_raw_data['col4'][new_ind],
                             'all_visits_this_year_april': fpc_raw_data['col5'][new_ind],
                             'all_visits_this_year_may': fpc_raw_data['col6'][new_ind],
                             'all_visits_this_year_june': fpc_raw_data['col7'][new_ind],
                             'all_visits_this_year_july': fpc_raw_data['col8'][new_ind],
                             'all_visits_this_year_august': fpc_raw_data['col9'][new_ind],
                             'all_visits_this_year_september': fpc_raw_data['col10'][new_ind],
                             'all_visits_this_year_october': fpc_raw_data['col11'][new_ind],
                             'all_visits_this_year_november': fpc_raw_data['col12'][new_ind],
                             'all_visits_this_year_december': fpc_raw_data['col13'][new_ind]}, ignore_index=True)
            #                 print(FPC_KPI_Metrics_By_Month_this_year)
            elif '#' in fpc_raw_data['col1'][ind] and 'Metrics' in fpc_raw_data['col1'][ind] and 'month - last' in \
                    fpc_raw_data['col1'][ind] and timelvlnm == 'Month':
                FPC_KPI_Metrics_By_Month_last_year = pd.DataFrame(
                    columns=['metrix', 'all_visits_last_year_january', 'all_visits_last_year_february',
                             'all_visits_last_year_march',
                             'all_visits_last_year_april', 'all_visits_last_year_may', 'all_visits_last_year_june',
                             'all_visits_last_year_july', 'all_visits_last_year_august',
                             'all_visits_last_year_september',
                             'all_visits_last_year_october', 'all_visits_last_year_november',
                             'all_visits_last_year_december'])
                for new_ind in fpc_raw_data.index[ind + 5:]:
                    if '####' in fpc_raw_data['col1'][new_ind]:
                        break
                    if '|' in fpc_raw_data['col1'][new_ind]:
                        record = f"{fpc_raw_data['col1'][new_ind]},{fpc_raw_data['col2'][new_ind]},{fpc_raw_data['col3'][new_ind]},{fpc_raw_data['col4'][new_ind]},{fpc_raw_data['col5'][new_ind]},{fpc_raw_data['col6'][new_ind]},{fpc_raw_data['col7'][new_ind]},{fpc_raw_data['col8'][new_ind]},{fpc_raw_data['col9'][new_ind]},{fpc_raw_data['col10'][new_ind]},{fpc_raw_data['col11'][new_ind]},{fpc_raw_data['col12'][new_ind]},{fpc_raw_data['col13'][new_ind]}"
                        FPC_KPI_Metrics_By_Month_last_year = FPC_KPI_Metrics_By_Month_last_year.append(
                            {'metrix': fpc_raw_data['col1'][new_ind],
                             'all_visits_last_year_january': fpc_raw_data['col2'][new_ind],
                             'all_visits_last_year_february': fpc_raw_data['col3'][new_ind],
                             'all_visits_last_year_march': fpc_raw_data['col4'][new_ind],
                             'all_visits_last_year_april': fpc_raw_data['col5'][new_ind],
                             'all_visits_last_year_may': fpc_raw_data['col6'][new_ind],
                             'all_visits_last_year_june': fpc_raw_data['col7'][new_ind],
                             'all_visits_last_year_july': fpc_raw_data['col8'][new_ind],
                             'all_visits_last_year_august': fpc_raw_data['col9'][new_ind],
                             'all_visits_last_year_september': fpc_raw_data['col10'][new_ind],
                             'all_visits_last_year_october': fpc_raw_data['col11'][new_ind],
                             'all_visits_last_year_november': fpc_raw_data['col12'][new_ind],
                             'all_visits_last_year_december': fpc_raw_data['col13'][new_ind]}, ignore_index=True)
            #                 print(FPC_KPI_Metrics_By_Month_last_year)
            elif '#' in fpc_raw_data['col1'][ind] and 'Metrics' in fpc_raw_data['col1'][ind] and 'quarter' in \
                    fpc_raw_data['col1'][ind] and timelvlnm == 'Quarter':
                FPC_KPI_Metrics_By_quarter = pd.DataFrame(
                    columns=['metrix', 'all_visits_last_year_1', 'all_visits_last_year_2', 'all_visits_last_year_3',
                             'all_visits_last_year_4', 'all_visits_this_year_1', 'all_visits_this_year_2',
                             'all_visits_this_year_3', 'all_visits_this_year_4'])
                for new_ind in fpc_raw_data.index[ind + 5:]:
                    if '####' in fpc_raw_data['col1'][new_ind]:
                        break
                    if '|' in fpc_raw_data['col1'][new_ind]:
                        record = f"{fpc_raw_data['col1'][new_ind]},{fpc_raw_data['col2'][new_ind]},{fpc_raw_data['col3'][new_ind]},{fpc_raw_data['col4'][new_ind]},{fpc_raw_data['col5'][new_ind]},{fpc_raw_data['col6'][new_ind]},{fpc_raw_data['col7'][new_ind]},{fpc_raw_data['col8'][new_ind]},{fpc_raw_data['col9'][new_ind]}"
                        FPC_KPI_Metrics_By_quarter = FPC_KPI_Metrics_By_quarter.append(
                            {'metrix': fpc_raw_data['col1'][new_ind],
                             'all_visits_last_year_1': fpc_raw_data['col2'][new_ind],
                             'all_visits_last_year_2': fpc_raw_data['col3'][new_ind],
                             'all_visits_last_year_3': fpc_raw_data['col4'][new_ind],
                             'all_visits_last_year_4': fpc_raw_data['col5'][new_ind],
                             'all_visits_this_year_1': fpc_raw_data['col6'][new_ind],
                             'all_visits_this_year_2': fpc_raw_data['col7'][new_ind],
                             'all_visits_this_year_3': fpc_raw_data['col8'][new_ind],
                             'all_visits_this_year_4': fpc_raw_data['col9'][new_ind]
                             }, ignore_index=True)
        # print(FPC_KPI_Metrics_By_quarter)

        # for row in FPC_KPI_Metrics.itertuples():
        #     timeid = 3
        #     rsid = 'rsid1'
        #     rs_name = rs_name
        #     metric_segments = row.metrix.split('|')[5]
        #     for x in range(2,4):
        #         if x == 2:
        #             timeid = year
        #         else:timeid = year -1
        #         col_value = row[x]
        #         row_value = f'{timeid},{rsid},{rs_name},{metric_segments},{col_value}'
        #         data.append(row_value)
        # print(data)
        if timelvlnm == 'Month':
            for row in FPC_KPI_Metrics_By_Month_last_year.itertuples():
                timeid = 3
                rsid = rsid
                rs_name = rs_name
                metric_segments = row.metrix
                if '|' in metric_segments:
                    match_index_df = calcmetric_df[calcmetric_df['name'].str.lower() == metric_segments.lower()]
                    if not match_index_df.empty:
                        metric_id = match_index_df['id'].values[0]
                    else:
                        metric_id = 0

                        print('Unable to fecth/find the col_name in calcmetric_df')
                    metric_segments_list = self.get_metric_name_desc_segments(check_lob, metric_segments)
                else:
                    metric_segments_list = self.get_metric_name_desc_segments_calcmetric_df(check_lob, metric_segments,
                                                                                            calcmetric_df)
                for x in range(1, 13):
                    if x < 10:
                        timeid = f'{year - 1}0{x}'
                    else:
                        timeid = f'{year - 1}{x}'
                    col_value = float(row[x + 1])
                    row_value = [timeid, rsid, rs_name] + metric_segments_list + [col_value] + [metric_id]
                    data.append(row_value)
            # print(data)
        if timelvlnm == 'Month':
            for row in FPC_KPI_Metrics_By_Month_this_year.itertuples():
                timeid = 3
                rsid = rsid
                rs_name = rs_name
                metric_segments = row.metrix
                if '|' in metric_segments:
                    match_index_df = calcmetric_df[calcmetric_df['name'].str.lower() == metric_segments.lower()]
                    if not match_index_df.empty:
                        metric_id = match_index_df['id'].values[0]
                    else:
                        metric_id = 0

                        print('Unable to fecth/find the col_name in calcmetric_df')

                    metric_segments_list = self.get_metric_name_desc_segments(check_lob, metric_segments)
                else:
                    metric_segments_list = self.get_metric_name_desc_segments_calcmetric_df(check_lob, metric_segments,
                                                                                            calcmetric_df)

                for x in range(1, 13):
                    if x < 10:
                        timeid = f'{year}0{x}'
                    else:
                        timeid = f'{year}{x}'
                    col_value = float(row[x + 1])
                    row_value = [timeid, rsid, rs_name] + metric_segments_list + [col_value] + [metric_id]
                    data.append(row_value)
            # print(data)
        if timelvlnm == 'Quarter':
            for row in FPC_KPI_Metrics_By_quarter.itertuples():
                timeid = 3
                rsid = rsid
                rs_name = rs_name
                metric_segments = row.metrix
                if '|' in metric_segments:
                    match_index_df = calcmetric_df[calcmetric_df['name'].str.lower() == metric_segments.lower()]
                    if not match_index_df.empty:
                        metric_id = match_index_df['id'].values[0]
                    else:
                        metric_id = 0

                        print('Unable to fecth/find the col_name in calcmetric_df')
                    metric_segments_list = self.get_metric_name_desc_segments(check_lob, metric_segments)
                else:
                    metric_segments_list = self.get_metric_name_desc_segments_calcmetric_df(check_lob, metric_segments,
                                                                                            calcmetric_df)
                for x in range(1, 9):
                    if x < 5:
                        timeid = f'{year - 1}{x}'
                    else:
                        timeid = f'{year}{x - 4}'
                    col_value = float(row[x + 1])
                    row_value = [timeid, rsid, rs_name] + metric_segments_list + [col_value] + [metric_id]
                    data.append(row_value)
            # print(data)

        print("data adding to list successful!")
        return data

    def createDF(self, data):
        print("creating schema..")
        try:
            schemaStr = StructType([
                StructField("timeid", StringType(), True),
                StructField("rpt_suite_id", StringType(), True),
                StructField("rpt_suite_nm", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("lob", StringType(), True),
                StructField("product", StringType(), True),
                StructField("category_type", StringType(), True),
                StructField("category", StringType(), True),
                StructField("metric_name", StringType(), True),
                StructField("metric_value", StringType(), True),
                StructField("metric_id", StringType(), True)
            ])
            df = spark.createDataFrame(data=data, schema=schemaStr)
        except:
            raise Exception("datatypes dont match!!")

        create_ts = self.curr_time()
        df = df.withColumn("create_ts", lit(create_ts)).withColumn("update_ts", lit(create_ts))
        #         print("printing final create df")
        #         print(df)
        return df

    def final_data_extract(self, df):
        df = df.withColumn("sgm_cd", lit("")).withColumn("sgm_desc", lit("")).withColumn("seg_cd", lit("")).withColumn(
            "seg_desc", lit("")).withColumn("metric_format", lit("")).withColumn("page_url", lit("")).withColumn(
            "data_src", lit(""))

        col_list = ["timelvl", "timelvlnm", "timeid", "timeidnm", "rpt_suite_id", "rpt_suite_nm", "channel", "lob",
                    "product", "category_type", "category", "sgm_cd", "sgm_desc", "seg_cd", "seg_desc", "metric_name",
                    "metric_value", "metric_format", "page_url", "data_src", "create_ts", "update_ts", "metric_id", ]
        df = df.select(*col_list)
        df.show(10)
        print("final data extract created!")
        return df

    def time_lvl_join(self, df, timelvlnm):
        print("im in timelevel")
        #         timelvl = self.timeLevel
        timelvl = timelvlnm
        print(timelvl)
        try:
            time_df = spark.sql("select * from sl_kpi.timelvl_timeid_drvr_std")
        #             print("printing time_df")
        #             time_df.show()
        except:
            raise Exception("time level view is not available")

        tim_df_filtered = time_df.filter(col("timelvlnm") == timelvl)
        #         tim_df_filtered = time_df
        #         print("printing tim_df_filtered")
        #         tim_df_filtered.show()
        df_updated = tim_df_filtered.alias('tim_df_filtered').join(df.alias('df'), tim_df_filtered.timeid == df.timeid,
                                                                   "inner").select('df.*', 'tim_df_filtered.timelvl',
                                                                                   'tim_df_filtered.timeidnm',
                                                                                   'tim_df_filtered.timelvlnm')
        #         print(df_updated.show(10))
        final_df = self.final_data_extract(df=df_updated)
        print(final_df.printSchema())
        print(final_df)
        print("time level join completed!")

        return final_df
