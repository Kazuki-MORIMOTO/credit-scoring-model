import io
import os
import sys

import pandas as pd
import boto3


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