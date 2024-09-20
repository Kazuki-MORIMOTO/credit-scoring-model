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
        
        ### 免許証データ
        self.df_indivisual_license = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_LICENSE, 
                                                   encoding="shift-jis")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_LICENSE,
                             self.df_indivisual_license.shape))
        
        ### 与信結果データ_カラム名
        self.df_indivisual_result_colname = read_csv_from_s3(bucket = config.S3_BUCKET,
                                                             key = config.KEY_INDIVISUAL_RAW_TFCDATA,
                                                             filename = config.FILENAME_INDIVISUAL_RAW_RESULT_COLNAME,
                                                             encoding="utf-8")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_RESULT_COLNAME,
                             self.df_indivisual_result_colname.shape))
        
        ### 説明変数データ_カラム名
        self.df_indivisual_explain_colname = read_csv_from_s3(bucket = config.S3_BUCKET,
                                                             key = config.KEY_INDIVISUAL_RAW_TFCDATA,
                                                             filename = config.FILENAME_INDIVISUAL_RAW_EXPLAIN_COLNAME,
                                                             encoding="utf-8")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_EXPLAIN_COLNAME,
                             self.df_indivisual_explain_colname.shape))
        
        ### 目的変数データ_カラム名
        self.df_indivisual_object_colname = read_csv_from_s3(bucket = config.S3_BUCKET,
                                                             key = config.KEY_INDIVISUAL_RAW_TFCDATA,
                                                             filename = config.FILENAME_INDIVISUAL_RAW_OBJECT_COLNAME,
                                                             encoding="utf-8")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_OBJECT_COLNAME,
                             self.df_indivisual_object_colname.shape))
        
        ### 免許証データ_カラム名
        self.df_indivisual_license_colname = read_csv_from_s3(bucket = config.S3_BUCKET,
                                                             key = config.KEY_INDIVISUAL_RAW_TFCDATA,
                                                             filename = config.FILENAME_INDIVISUAL_RAW_LICENSE_COLNAME,
                                                             encoding="utf-8")
        print("{}:{}".format(config.FILENAME_INDIVISUAL_RAW_LICENSE_COLNAME,
                             self.df_indivisual_license_colname.shape))
        
        
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
    def rename_columns_tfc_data(self,):
        ### Creating a column name dictionary
        dict_result_colname_mapping = dict(zip(self.df_indivisual_result_colname['カラム名'], 
                                                self.df_indivisual_result_colname['カラム名（和名）']))
        dict_explain_colname_mapping = dict(zip(self.df_indivisual_explain_colname['カラム名'], 
                                                self.df_indivisual_explain_colname['カラム名（和名）']))
        dict_object_colname_mapping = dict(zip(self.df_indivisual_object_colname['カラム名'], 
                                                self.df_indivisual_object_colname['カラム名（和名）']))
        dict_license_colname_mapping = dict(zip(self.df_indivisual_license_colname['カラム名'], 
                                                self.df_indivisual_license_colname['カラム名（和名）']))
        
        ### Rename columns
        self.df_indivisual_result = self.df_indivisual_result.rename(columns = dict_result_colname_mapping)
        self.df_indivisual_result.columns = [f"{col}_result" for col in self.df_indivisual_result.columns]
        
        self.df_indivisual_explain = self.df_indivisual_explain.rename(columns = dict_explain_colname_mapping)
        self.df_indivisual_explain.columns = [f"{col}_result" for col in self.df_indivisual_explain.columns]
        
        self.df_indivisual_object = self.df_indivisual_object.rename(columns = dict_object_colname_mapping)
        self.df_indivisual_object.columns = [f"{col}_result" for col in self.df_indivisual_object.columns]
        
        self.df_indivisual_license = self.df_indivisual_license.rename(columns = dict_license_colname_mapping)
        self.df_indivisual_license.columns = [f"{col}_result" for col in self.df_indivisual_license.columns]
        
    """
    Delete unnecessary records
    """
    
    
    """
    Cast a pandas object to a specified dtype
    """
    
    
    """
    Merge result and explain and object and license
    """
    
    
    
    
    """
    preprocess pipelines
    """
    def preprocess_pipeline(self,):
        self.read_data_from_raw()
        self.concat_result_and_explain_data_respectively()
        self.rename_columns_tfc_data()
        
        



if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    TFCProcessingDataPreprocessor = TFCProcessingDataPreprocessor()
    TFCProcessingDataPreprocessor.preprocess_pipeline()
    
    
    