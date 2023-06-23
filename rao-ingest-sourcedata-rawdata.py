import json
import boto3
import datetime as d
import logging
import os
import re
from aws_lambda_powertools import Logger

currentday = d.datetime.now()
date_only = currentday.date()
time_stamp = currentday.strftime('%H:%M:%S')
date = currentday.day
month = currentday.month
year = currentday.year
hour = currentday.hour
minute = currentday.minute
second = currentday.second

s3c = boto3.client('s3')
s3r = boto3.resource('s3')
sns_client = boto3.client('sns')

log = Logger(service = "movielens")

def email(sub, msg):
    response = sns_client.publish(
        TopicArn=os.environ['sns_topic_arn'],
        Subject = sub,
        Message = msg
    )

def lambda_handler(event, context):
    
    try:
        obj = s3r.Object('rao-data-ingestion-config-code', f"data-ingestion/config/ingest_config.json")
        obj_response = obj.get()['Body'].read().decode('utf-8')
        obj_res_dict = json.loads(obj_response)
        folder = obj_res_dict.get("data_set")
        schedule = obj_res_dict.get("schedule")
        pipeline = obj_res_dict.get("pipeline")
        
        email("Reading 'ingest_config' is successfull",
        f"The config file returned folder as {folder}, schedule as {schedule} and pipeline as {pipeline}"
        )
    
    except Exception as e:
        # print(e)
        email("Reading 'ingest_config' failure",
        f"The congig file reading is failed with error '{e}'"
        )
        
    try:
        obj_list = list()
        file_list = list()
        copied_files_list = list()
        
        for configuration in pipeline:
            file_config = configuration.get("raw")
            source_bucket = file_config.get("source_bucket")
            list_of_s3_objects = s3c.list_objects_v2(Bucket=source_bucket)["Contents"]
            for key in list_of_s3_objects:
                 obj_list.append(key)
        
        for obj in obj_list:
            obj_name = obj.get('Key')
            
            if obj_name.split('/')[1] != '' and obj_name.split('/')[1] not in file_list:
               file_list.append(obj_name.split('/')[1])
        # print(file_list)
        
        email("Getting s3 Source Bucket objects is successfull",
        f"The s3 Source Bucket objects list is {file_list}"
        )
    
    except Exception as e:
        #print(e)
        email("Getting s3 Source Bucket objects is failed",
        f"Getting s3 Source Bucket objects list is failed with error '{e}'"
        )
        
    try:
            
        for configuration in pipeline:
            file_config = configuration.get("raw")
            file_pattern = file_config.get("file_pattern")
            file_extension = file_config.get("file_type")
            source_bucket = file_config.get("source_bucket")
            target_bucket = file_config.get("target_bucket")
            source_folder = file_config.get("source_folder")
            partition = file_config.get("partition")
            
            for file in file_list:
                x = re.search(file_pattern, file)
            
                if x:
                    file_name = file.split('.')[0]
                    #print(f"{source_bucket} -- {target_bucket} -- {partition} -- {source_folder} - {file_name}")
                
                    if partition == "DAY":
                        file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        #print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                
                    elif partition == 'MONTH':
                        file_path = f"{source_folder}/{file_name}/year={year}/month={month}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        #print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                    
                    elif partition == "YEAR":
                        file_path = f"{source_folder}/{file_name}/year={year}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        #print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                    
                    elif partition == "HOUR":
                        file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        #print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                    
                    elif partition == "MINUTE":
                        file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/minute={minute}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        #print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                
                    elif partition == "SECOND":
                        file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/minute={minute}/second={second}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                        print(file_path)
                        copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                        bucket = s3r.Bucket(target_bucket)
                        bucket.copy(copy_source, file_path)
                        copied_files_list.append(file_path)
                
                    else:
                        # print(f"{file_pattern} does not exist in {file_list}")
                        email(f"{file_pattern}.{file_extension} does not exit",
                        f"The file {file_pattern}.{file_extension} does't exist in s3 bucket {source_bucket}"
                        )
                
        email("Files copy is successful",
              f"The existing files {copied_files_list} are copied success fully"
        )
        
    except  Exception as e:
        #print(e)
        email("Files copy failed",
              f"Copying of file from {source_bucket} to {target_bucket} failed with error '{e}'"
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'copied_list':copied_files_list})
    }