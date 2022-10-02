from helper import kpiAPICall
from helper import kpiCommonFramework
from helper import kpiFormDataStructure
from helper import kpiWriteToDelta
import sys

if __name__ == '__main__':
    print("Start code consolidation Job")
    code_exception = False
    try:
        ##apicall class called
        api_instance = kpiAPICall.APICall(job_config_path = sys.argv[1], api_config_path = sys.argv[2], cf_creds_path = sys.argv[3], jobName = sys.argv[4])

        ##common framework class called
        common_framework = kpiCommonFramework.commonFramework()
        token = common_framework.generateToken()

        print("Job Started")
        jobstart = common_framework.jobStart(token)

        code_exception = False

        if jobstart['response'] == True:
            code_exception = True
            login_Var = api_instance.login("/dbfs/tmp/config_path/api_config_uhg.json")
            # json_obj = api_instance.request_call()

            ## form datastructure class called
            raw_df = kpiFormDataStructure.formDataStructure(api_instance)
            calcmetric_df = raw_df.get_calcmetric_mapping()
            raw_csv = raw_df.request_call()
            data = raw_df.form_data_structure(fpc_raw_data=raw_csv, calcmetric_df = calcmetric_df,timelvlnm=timelvlnm)
            dataframe = raw_df.createDF(data = data)
            final_df = raw_df.time_lvl_join(df = dataframe, timelvlnm=timelvlnm)

            ## write to delta class called
            silver_df = kpiWriteToDelta.writeToDelta(raw_df)
            silver_df.write_delta(spark, dataFrame = final_df)
            # silver_df.create_view(spark)

        else:
            raise Exception()

        jobend = common_framework.jobEnd(token)
        if jobend['response'] == True:
            print("Job Succeeded!")

        else:
            raise Exception()

    except Exception as e:
        print(code_exception)

        if code_exception == False:
            raise Exception("Job start or end Failed!")
        else:
            print("Job Failed")
            print(str(e))
            ##apicall class called
            api_instance = APICall(job_config_path = sys.argv[1], api_config_path = sys.argv[2], cf_creds_path = sys.argv[3], jobName = sys.argv[4])
            ##common framework class called
            common_framework = kpiCommonFramework.commonFramework()

            token = common_framework.generateToken()
            jobfail = common_framework.jobFail(token)
            raise Exception()
