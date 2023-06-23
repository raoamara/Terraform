import json
import boto3
import datetime as d
import logging
import os

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

logging.basicConfig(
level = logging.INFO,
format = f"%(asctime)s %(lineno)d %(levelname)s %(message)s",
)

log = logging.getLogger("Ingest-Raw")
log.info("")
log.setLevel(logging.INFO)


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
        #print(folder, schedule, pipeline)
        email("Reading 'ingest_config' is successfull", 
              f"The config file returned folder as {folder}, schedule as {schedule} and pipeline as {pipeline}")
    
    except Exception as e:
        email("Reading 'ingest_config' failure",
              f"The congig file reading is failed with error '{e}'"
        )
        
    try:
        
        copied_files_list = []
        
        for configuration in pipeline:
            file_name = configuration.get("data_asset")
            file_config = configuration.get("raw")
            file_extension = file_config.get("file_type")
            source_bucket = file_config.get("source_bucket")
            target_bucket = file_config.get("target_bucket")
            source_folder = file_config.get("source_folder")
            partition = file_config.get("partition")
            #print(f"{file_name} -- {file_extension} -- {source_bucket} -- {target_bucket} -- {source_folder} -- {partition}")
            
            if  partition == "DAY":
                file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
                
            elif partition == "MONTH":
                file_path = f"{source_folder}/{file_name}/year={year}/month={month}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
                 
            elif partition == "YEAR":
                file_path = f"{source_folder}/{file_name}/year={year}/{file_name}_{date_only}T{time_stamp}.{file_extension}"
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
            
            elif partition == "HOUR":
                file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/{file_name}_{date_only}T{time_stamp}.{file_extension}" 
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
                
            elif partition == "MINUTE":
                file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/minute={minute}/{file_name}_{date_only}T{time_stamp}.{file_extension}" 
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
                
            elif partition == "SECOND":
                file_path = f"{source_folder}/{file_name}/year={year}/month={month}/day={date}/hour={hour}/minute={minute}/second={second}/{file_name}_{date_only}T{time_stamp}.{file_extension}" 
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_folder}/{file_name}.{file_extension}"}
                bucket = s3r.Bucket(target_bucket)
                bucket.copy(copy_source, file_path)
                copied_files_list.append(file_path)
                
            else:
                print("Not a valid partition")
                
        email("Files copy is successful",
               f"The files {copied_files_list} are copied success fully"
        )
        
    except  Exception as e:
        email("Retrieval of file attributes failure",
              f"The pipeline attributes retrival failed with error '{e}'"
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'copied_list':copied_files_list})
    }