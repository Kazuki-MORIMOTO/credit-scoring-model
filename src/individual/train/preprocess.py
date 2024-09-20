import io
import os
import time

import pandas as pd

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3

"""
Preprocessing class for TFC processing data
"""
class TFCProcessingDataPreprocessor:
    def __init__(self,):
        print(" Start of preprocessing of TFC processing data")
    
    """
    read tfc data
    """
    def read_data_from_raw(self,):
        ### 与信結果
        self.df_indivisual_result_to202312 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_RESULT_TO202312, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_RESULT_TO202312,
                             self.df_indivisual_result_to202312.shape))
        self.df_indivisual_result_to202405 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_RESULT_TO202405, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_RESULT_TO202405,
                             self.df_indivisual_result_to202405.shape))
        
        ### 説明変数
        self.df_indivisual_explain_to202312 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_EXPLAIN_TO202312, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_EXPLAIN_TO202312,
                             self.df_indivisual_explain_to202312.shape))
        self.df_indivisual_explain_to202405 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_EXPLAIN_TO202405, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_EXPLAIN_TO202405,
                             self.df_indivisual_explain_to202405.shape))
        
        ### 目的変数
        self.df_indivisual_object = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_OBJECT, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_OBJECT,
                             self.df_indivisual_object.shape))
        
        ### 与信結果データ_カラム名
        self.df_indivisual_result_colname = read_csv_from_s3(bucket = config.S3_BUCKET,
                                                             key = config.KEY_INDIVISUAL_RAW_TFCDATA,
                                                             filename = config.FILENAME_INDIVISUAL_RESULT_COLNAME,
                                                             encoding="utf-8")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_RESULT_COLNAME,
                             self.df_indivisual_result_colname.shape))
        
        ### 説明変数データ_カラム名
        
        ### 目的変数データ_カラム名
        
        
    """
    concat result data / explain data
    """
    def concat_result_and_explain_data_respectively(self,):
        ### 与信結果データ
        self.df_indivisual_result = pd.concat([self.df_indivisual_result_to202312,
                                               self.df_indivisual_result_to202405])
        print("df_indivisual_result shape:{}".format(self.df_indivisual_result.shape))
        
        ### 説明変数データ
        self.df_indivisual_explain = pd.concat([self.df_indivisual_explain_to202312,
                                               self.df_indivisual_explain_to202405])
        print("df_indivisual_explain shape:{}".format(self.df_indivisual_explain.shape))
        
    """
    rename columns
    """
#     def rename_columns(self,):
        
    
    def preprocess_pipeline(self,):
        self.read_data_from_raw()
        self.concat_result_and_explain_data_respectively()
        
        



if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    TFCProcessingDataPreprocessor = TFCProcessingDataPreprocessor()
    TFCProcessingDataPreprocessor.preprocess_pipeline()
    
    