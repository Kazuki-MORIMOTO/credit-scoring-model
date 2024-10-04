import io
import os
import sys
import time

import pandas as pd
import boto3
import pprint
import datetime


__author__ = 'kazuki morimoto'
__doc__ = '''this py file is the collection of those functions or methods used in many other modules'''


"""
loads a configuration file based on the value of the CONFIG environment variable.
"""
def load_config():
    config_name = os.getenv('CONFIG', 'dev')  
    if config_name == 'prod':
        import conf.prod as config
    elif config_name == 'stg':
        import conf.stg as config
    else:
        import conf.dev as config
    return config


"""
read csv from s3
"""
def read_csv_from_s3(bucket: str, key: str, filename: str, encoding: str):
    filepath = os.path.join(key, filename)
    response = boto3.resource("s3").Bucket(bucket).Object(key=filepath).get()
    df = pd.read_csv(io.BytesIO(response["Body"].read()), 
                     encoding = encoding,
                     index_col=None)
    return df

""" 
output csv to s3
"""
def output_csv_for_s3(bucket: str, key: str, filename: str, df: pd.DataFrame()):
    print("start save to :{}".format(key+filename))
    boto3.resource("s3").Bucket(bucket).put_object(Key=key+"/"+filename, 
                                                   Body=df.to_csv(index=False))
    print("complete save to :{}".format(key+filename))
    
"""
Get latest file from s3
"""
def get_latest_file(bucket: str, prefix: str):
    ### List objects in S3 bucket
    response = boto3.client('s3').list_objects_v2(Bucket=bucket, 
                                         Prefix=prefix)

    if 'Contents' in response:
        # 最新のファイルを取得
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        file_name = os.path.basename(latest_file['Key'])
        return file_name
    else:
        return None
    

"""
Get data from athena and save it to S3

query_athena_to_s3()
- athena_client: function
- sql: str
- database: str
- bucket: str
- key: str
"""
def query_athena_to_s3(athena_client: boto3.Session.client, sql: str, database: str, bucket: str, key: str) -> pd.DataFrame:
    ### クエリ実行の開始
    output_location = os.path.join("s3://", bucket, key)
    response = athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={
                    "Database":database
                },
                ResultConfiguration={
                    "OutputLocation":output_location
            })
    print("-- Check start_query_execution`s response:")
    pprint.pprint(response)
    
    ### クエリ実行状況の確認
    query_execution_id = response["QueryExecutionId"]
    start_time = time.time()
    while True:
        elapsed_time = time.time() - start_time
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print(state)
            break
        else:
            print("wait...")
            time.sleep(5)
            
    ### クエリの実行結果の確認
    filename = os.path.join(key,"{}.csv".format(query_execution_id))
    response = boto3.resource("s3").Bucket(bucket).Object(key=filename).get()
    df = pd.read_csv(io.BytesIO(response["Body"].read()), encoding="utf-8")
    today = datetime.date.today()
    today_str = today.strftime('%Y-%m-%d')
    boto3.resource("s3").Bucket(bucket).put_object(Key=key+"/"+today_str+".csv", Body=df.to_csv())
    
    return df