import io
import os
import time

import numpy as np
import pandas as pd

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3
from src.common.common_func import get_latest_file

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
        self.df_indivisual_explain.columns = [f"{col}_explain" for col in self.df_indivisual_explain.columns]
        
        self.df_indivisual_object = self.df_indivisual_object.rename(columns = dict_object_colname_mapping)
        self.df_indivisual_object.columns = [f"{col}_object" for col in self.df_indivisual_object.columns]
        
        self.df_indivisual_license = self.df_indivisual_license.rename(columns = dict_license_colname_mapping)
        self.df_indivisual_license.columns = [f"{col}_license" for col in self.df_indivisual_license.columns]
        
    """
    Delete unnecessary columns
    """
    def delete_unnecessary_columns(self, ):
        ### drop duplicated records
        print("df_indivisual_result shape:{}".format(self.df_indivisual_result.shape))
        self.df_indivisual_result = self.df_indivisual_result.loc[:, ~self.df_indivisual_result.columns.duplicated()]
        print("df_indivisual_result shape:{}".format(self.df_indivisual_result.shape))
        
    
    """
    Cast a pandas object to a specified dtype
    """
    def change_dataframe_dtype(self, ):
        ### 契約番号 for object data 
        self.df_indivisual_object["契約番号_object"] = self.df_indivisual_object["契約番号_object"].astype("int").astype(str)
        print("df_indivisual_object nunique 契約番号_object:{}".format(self.df_indivisual_object["契約番号_object"].nunique()))
        
        ### TFC加工データ　免許証データ
        self.df_indivisual_license["免許証番号_license"] = self.df_indivisual_license["免許証番号_license"].astype(str).str.replace('.0', '', regex=False)
        print("df_indivisual_license nunique 免許証番号_license:{}".format(self.df_indivisual_license["免許証番号_license"].nunique()))
        
    """
    Create datetime columns
    """
    def YYYYMM_convert_to_datetime(self, x):
        if pd.isnull(x):
            return np.nan  
        else:
            return pd.to_datetime(str(int(x)), format='%Y%m')
        
    def YYYYMMDD_convert_to_datetime(self, x):
        if pd.isnull(x):
            return np.nan  
        else:
            return pd.to_datetime(str(int(x)), format='%Y%m%d')
        
    def create_datetime_columns(self, ):
        ### new datetime columns
        self.df_indivisual_result["一次審査完了年月日時間_datetime"] = pd.to_datetime(self.df_indivisual_result["一決＿日_result"].astype(str) \
                                                                           + self.df_indivisual_result["一決＿時分秒_result"].astype(str),
                                                                           format='%Y%m%d%H%M%S')
        ### Convert float type to datetime type for YYYYMM
        target_col_list = ["受入年月_object", "初回４次延滞発生年月_object",]
        for col in target_col_list:
            self.df_indivisual_object[col+"_datetime"] = self.df_indivisual_object[col].apply(self.YYYYMM_convert_to_datetime)
            
        ### Convert float type to datetime type for YYYYMMDD
        target_col_list = ["期失処理日_object"]
        for col in target_col_list:
            print(col)
            self.df_indivisual_object[col+"_datetime"] = self.df_indivisual_object[col].apply(self.YYYYMMDD_convert_to_datetime)
    
        
    """
    Merge result and explain and object and license
    """
    def merge_tfc_data(self, ):
        ### 事前チェック
        # print("{}:{}".format("与信結果", df_personal_result.shape))
        # print("{}:{}".format("説明変数", df_personal_explain.shape))
        # print("{}:{}".format("目的変数", df_personal_object.shape))
        # print("{}:{}".format("免許証", df_personal_license.shape))

        ### 与信結果　merge 説明変数
        self.df_tfc = pd.merge(self.df_indivisual_result, 
                               self.df_indivisual_explain,
                               left_on = ["申込書受付番号_result"],
                               right_on = ["申込書受付番号_explain"],
                               how="left"
                              )
        print("merge result and explain:{} , nunique 申込書受付番号_result:{}"\
              .format(self.df_tfc.shape,
                      self.df_tfc["申込書受付番号_result"].nunique()))

        ### tfc 目的変数データ
        self.df_tfc = pd.merge(self.df_tfc,
                               self.df_indivisual_object,
                               left_on = ["申込書受付番号_result"],
                               right_on = ["申込書受付番号_object"], 
                               how = "left")
        print("merge tfc and object:{}, nunique 申込書受付番号_result:{}".format(self.df_tfc.shape, 
                                                                          self.df_tfc["申込書受付番号_result"].nunique()))
        print("merge tfc and object:{}, nunique 契約番号_object:{}".format(self.df_tfc.shape,
                                                                       self.df_tfc["契約番号_object"].nunique()))

        ### merge 免許証データ
        self.df_tfc = pd.merge(self.df_tfc,
                               self.df_indivisual_license,
                               left_on = ["申込書受付番号_result"],
                               right_on = ["申込書受付番号_license"], 
                               how = "left")
        print("merge tfc and license:{}, nunique 申込書受付番号_result:{}".format(self.df_tfc.shape, 
                                                                           self.df_tfc["申込書受付番号_result"].nunique()))
        
    """
    Remove null license number
    """
    def remove_null_licence_reocords(self, ):
        print("df_tfc shape: {}".format(self.df_tfc.shape))
        self.df_tfc = self.df_tfc.query("免許証番号_license != 'nan'")
        print("df_tfc shape: {}".format(self.df_tfc.shape))
        
    """
    Create the order of applications
    """
    def create_order_of_applications(self, ):
        self.df_tfc = self.df_tfc.sort_values(["免許証番号_license","一次審査完了年月日時間_datetime"], 
                                              ascending=[True, True])
        self.df_tfc["一次審査完了順"] = self.df_tfc.groupby("免許証番号_license").cumcount()+1
    
    """
    preprocess pipelines
    """
    def preprocess_pipeline(self,):
        ### preprocess_1
        self.read_data_from_raw()
        self.concat_result_and_explain_data_respectively()
        self.rename_columns_tfc_data()
        self.delete_unnecessary_columns()
        self.change_dataframe_dtype()
        self.create_datetime_columns()
        
        ### merge
        self.merge_tfc_data()
        
        ### preprocess_2
        self.remove_null_licence_reocords()
        self.create_order_of_applications()

        
"""
Preprocessing class for KINTO Inner data
"""
class KINTOInnerDataPreprocessor:
    def __init__(self,):
        print(" Start of preprocessing of KINTO Inner data")
        
    """
    read KINTO Inner data
    """
    def read_data_from_raw(self,):
        ### Car Data
        latest_filename_CarData = get_latest_file(bucket = config.S3_BUCKET,
                                           prefix = config.KEY_INDIVISUAL_RAW_CAR_DATA)
        print("Latest file name of car data : {}".format(latest_filename_CarData))
        self.df_CarData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_CAR_DATA, 
                                           filename = latest_filename_CarData, 
                                           encoding = "utf-8")
        print("{}:{}".format(latest_filename_CarData, self.df_CarData.shape))
        
        ### Chomonix Data
        latest_filename_ChomonixData = get_latest_file(bucket = config.S3_BUCKET,
                                                   prefix = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA)
        print("Latest file name of chomonix data : {}".format(latest_filename_ChomonixData))
        self.df_ChomonixData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA, 
                                           filename = latest_filename_ChomonixData, 
                                           encoding = "utf-8")
        print("{}:{}".format(latest_filename_ChomonixData, self.df_ChomonixData.shape))
        
        ### Weblog Data
        latest_filename_WeblogData = get_latest_file(bucket = config.S3_BUCKET,
                                                   prefix = config.KEY_INDIVISUAL_RAW_WEBLOG_DATA)
        print("Latest file name of weblog data : {}".format(latest_filename_WeblogData))
        self.df_WeblogData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_WEBLOG_DATA, 
                                           filename = latest_filename_WeblogData, 
                                           encoding = "utf-8")
        print("{}:{}".format(latest_filename_WeblogData, self.df_WeblogData.shape)) 
        
    """
    preprocess pipelines
    """
    def preprocess_pipeline(self,):
        ### preprocess_1
        self.read_data_from_raw()
        
        
if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    # TFCProcessingDataPreprocessor = TFCProcessingDataPreprocessor()
    # TFCProcessingDataPreprocessor.preprocess_pipeline()
    
    KINTOInnerDataPreprocessor = KINTOInnerDataPreprocessor()
    KINTOInnerDataPreprocessor.preprocess_pipeline()
    