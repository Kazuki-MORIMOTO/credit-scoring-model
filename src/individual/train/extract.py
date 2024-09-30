import io
import os
import time

import pandas as pd

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3

"""
Reading TFC processing data
"""
def read_tfc_processing_data():
    ### 【個人】_与信結果データ 
    df_indivisual_result_to202312 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TRAINING, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_RESULT_TO202312, 
                                                   encoding="shift-jis")
    print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_RESULT_TO202312,
                         df_indivisual_result_to202312.shape))
    
    return df_indivisual_result_to202312

"""
Reading and saving application data
"""
def read_application_data() -> pd.DataFrame:
    ### read sql src
    with open(config.FILEPATH_SQL_APPLICATION_DATA, 'r') as file:
        sql_script = file.read()
    
    ### read application data from athena
    df_ApplicationData = query_athena_to_s3(athena_client = config.ATHENA_CLIENT,
                                                sql = sql_script, 
                                                database = config.DATABASE, 
                                                bucket = config.S3_BUCKET,
                                                key = config.KEY_INDIVISUAL_RAW_APPLICATION_DATA)
    print("df_ApplicationData shape:{}".format(df_ApplicationData.shape))
    
    ### save car data to s3
    output_csv_for_s3(bucket = config.S3_BUCKET, 
                      key = config.KEY_INDIVISUAL_RAW_APPLICATION_DATA, 
                      filename = config.FILENAME_APPLICATION_DATA, 
                      df = df_ApplicationData)
    
    return df_ApplicationData




"""
Reading and saving　Car data
"""
def read_car_data() -> pd.DataFrame:
    ### read sql src
    with open(config.FILEPATH_SQL_CAR_DATA, 'r') as file:
        sql_script = file.read()
    
    ### read car data from athena
    df_CarData = query_athena_to_s3(athena_client = config.ATHENA_CLIENT,
                                                sql = sql_script, 
                                                database = config.DATABASE, 
                                                bucket = config.S3_BUCKET,
                                                key = config.KEY_INDIVISUAL_RAW_CAR_DATA)
    print("df_CarData shape:{}".format(df_CarData.shape))
    
    ### save car data to s3
    output_csv_for_s3(bucket = config.S3_BUCKET, 
                      key = config.KEY_INDIVISUAL_RAW_CAR_DATA, 
                      filename = config.FILENAME_CAR_DATA, 
                      df = df_CarData)
    
    return df_CarData

"""
Reading and saving chomonix data
"""
def read_chomonix_data() -> pd.DataFrame:
    ### read sql src
    with open(config.FILEPATH_SQL_CHOMONIX_DATA, 'r') as file:
        sql_script = file.read()
    
    ### read car data from athena
    df_ChomonixData = query_athena_to_s3(athena_client = config.ATHENA_CLIENT,
                                                sql = sql_script, 
                                                database = config.DATABASE, 
                                                bucket = config.S3_BUCKET,
                                                key = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA)
    print("df_ChomonixData shape:{}".format(df_ChomonixData.shape))
    
    ### save chomonix data to s3
    output_csv_for_s3(bucket = config.S3_BUCKET, 
                      key = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA, 
                      filename = config.FILENAME_CHOMONIX_DATA, 
                      df = df_ChomonixData)
    
    return df_ChomonixData

"""
Reading and saving weblog data
"""
def read_weblog_data() -> pd.DataFrame:
    ### read weblog data
    df_Weblog = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                   key=config.KEY_INDIVISUAL_RAW_WEBLOG_DATA, 
                                   filename=config.FILENAME_INDIVISUAL_RAW_WEBLOG_DATA, 
                                   encoding="utf-8")
    print("df_Weblog shape:{}".format(df_Weblog.shape))
    
    ### save weblog data to s3
    output_csv_for_s3(bucket = config.S3_BUCKET, 
                      key = config.KEY_INDIVISUAL_RAW_WEBLOG_DATA, 
                      filename = config.FILENAME_WEBLOG_DATA, 
                      df = df_Weblog)
    return df_Weblog

"""
Reading and saving weblog data
"""
def read_kinto_licence_data() -> pd.DataFrame:
    ### read weblog data
    df_KINTOLicenceData = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                   key=config.KEY_INDIVISUAL_RAW_KINTO_LICENCE_DATA, 
                                   filename=config.FILENAME_INDIVISUAL_RAW_KINTO_LICENCE_DATA, 
                                   encoding="utf-8")
    print("df_KINTOLicenseData shape:{}".format(df_KINTOLicenceData.shape))
    
    ### save weblog data to s3
    output_csv_for_s3(bucket = config.S3_BUCKET, 
                      key = config.KEY_INDIVISUAL_RAW_KINTO_LICENCE_DATA, 
                      filename = config.FILENAME_KINTO_LICENCE_DATA, 
                      df = df_KINTOLicenceData)
    return df_KINTOLicenceData


if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    # ### TFC加工データの読み込み
    # df = read_tfc_processing_data()
    # print(df.shape)
    
    ### read application data from athena
    df_ApplicationData = read_application_data()
    
    ### read car_data from athena
    df_CarData = read_car_data()
    
    ### read chomonix data from athena
    df_ChomonixData = read_chomonix_data()
    
    ### read weblog data
    df_WeblogData = read_weblog_data()
    
    ### read KINTO licence data
    df_KINTOLicenceData= read_kinto_licence_data()