import io
import os
import time

import numpy as np
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3
from src.common.common_func import get_latest_file

"""
Cleansing class for TFC processing data
"""
class TFCProcessingDataCleaner:
    def __init__(self,):
        print(" Start of cleansing of TFC processing data")
    
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
    cleansing pipelines
    """
    def cleansing_pipeline(self,):
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
        
        return self.df_tfc
        

        
"""
Cleansing class for KINTO Inner data
"""
class KINTOInnerDataCleaner:
    def __init__(self,):
        print(" Start of cleansing of KINTO Inner data \n")
        
    """
    read KINTO Inner data
    """
    def read_data_from_raw(self,):
        ### Application Data
        latest_filename_ApplicationData = get_latest_file(bucket = config.S3_BUCKET,
                                           prefix = config.KEY_INDIVISUAL_RAW_APPLICATION_DATA)
        print("Latest file name of Application data : {} ".format(latest_filename_ApplicationData))
        self.df_ApplicationData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_APPLICATION_DATA, 
                                           filename = latest_filename_ApplicationData, 
                                           encoding = "utf-8")
        print("{}:{}\n".format(latest_filename_ApplicationData, self.df_ApplicationData.shape))
        
        ### Car Data
        latest_filename_CarData = get_latest_file(bucket = config.S3_BUCKET,
                                           prefix = config.KEY_INDIVISUAL_RAW_CAR_DATA)
        print("Latest file name of car data : {}".format(latest_filename_CarData))
        self.df_CarData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_CAR_DATA, 
                                           filename = latest_filename_CarData, 
                                           encoding = "utf-8")
        print("{}:{} \n".format(latest_filename_CarData, self.df_CarData.shape))
        
        ### Chomonix Data
        latest_filename_ChomonixData = get_latest_file(bucket = config.S3_BUCKET,
                                                   prefix = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA)
        print("Latest file name of chomonix data : {}".format(latest_filename_ChomonixData))
        self.df_ChomonixData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_CHOMONIX_DATA, 
                                           filename = latest_filename_ChomonixData, 
                                           encoding = "utf-8")
        print("{}:{} \n".format(latest_filename_ChomonixData, self.df_ChomonixData.shape))
        
        ### Weblog Data
        latest_filename_WeblogData = get_latest_file(bucket = config.S3_BUCKET,
                                                   prefix = config.KEY_INDIVISUAL_RAW_WEBLOG_DATA)
        print("Latest file name of weblog data : {}".format(latest_filename_WeblogData))
        self.df_WeblogData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_WEBLOG_DATA, 
                                           filename = latest_filename_WeblogData, 
                                           encoding = "utf-8")
        print("{}:{} \n".format(latest_filename_WeblogData, self.df_WeblogData.shape)) 
        
        ### KINTO Licence Data
        latest_filename_KINTOLicenceData = get_latest_file(bucket = config.S3_BUCKET,
                                                   prefix = config.KEY_INDIVISUAL_RAW_KINTO_LICENCE_DATA)
        print("Latest file name of KINTO Licence data : {}".format(latest_filename_KINTOLicenceData))
        self.df_KINTOLicenceData  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                           key = config.KEY_INDIVISUAL_RAW_KINTO_LICENCE_DATA, 
                                           filename = latest_filename_KINTOLicenceData, 
                                           encoding = "utf-8")
        print("{}:{} \n".format(latest_filename_KINTOLicenceData, self.df_KINTOLicenceData.shape)) 
        
        
    """
    Cast a pandas object to a specified dtype
    """
    def change_dataframe_dtype(self, ):
        ### 契約管理番号(TFC設定項目) for CarData
        self.df_CarData["契約管理番号(TFC設定項目)"] = self.df_CarData["契約管理番号(TFC設定項目)"].astype(str)
        self.df_CarData["契約管理番号(TFC設定項目)"] = self.df_CarData["契約管理番号(TFC設定項目)"].str.replace('.0', '', regex=False)
        
        self.df_CarData["与信審査申込日時"] = self.df_CarData["与信審査申込日時"].astype(str)
        self.df_CarData["与信審査申込日時"] = self.df_CarData["与信審査申込日時"].str.replace('.000', '', regex=False)

        print("df_CarData nunique 契約管理番号:{}".format(self.df_CarData["契約管理番号(TFC設定項目)"].nunique()))
        
        ### 運転免許番号 for KINTO Lincence data
        self.df_KINTOLicenceData["運転免許番号"] = self.df_KINTOLicenceData["運転免許番号"].astype(str).str.replace('.0', '', regex=False)
        print("df_KINTOLicenceData nunique 運転免許番号:{}".format(self.df_KINTOLicenceData["運転免許番号"].nunique()))
        
    """
    Create datetime columns
    """
    def create_datetime_columns(self, ):
        self.df_CarData["与信審査申込日時_datetime"] = pd.to_datetime(self.df_CarData["与信審査申込日時"],
                                        format='%Y-%m-%d %H:%M:%S')
    
    """
    Merge Application and Car and Chomonix and Weblog and KINTO Licence
    """
    def merge_kinto_data(self, ):
        ### merge Car data and Application data
        self.df_kinto = pd.merge(self.df_CarData,
                                self.df_ApplicationData.drop(["与信審査申込日時","本契約申込日時(契約日)","パッケージ表示名","グレード名","契約ステータス"], 
                                                    axis=1),
                                left_on = ["契約ID"],
                                right_on = ["契約ID"],
                                how="left")
        print("merge df_CarData and df_ApplicationData:{}, nunique 契約ID:{}".format(self.df_kinto.shape, 
                                                                                   self.df_kinto["契約ID"].nunique()))

        ### merge Weblog
        self.df_kinto = pd.merge(self.df_kinto,
                                self.df_WeblogData,
                                left_on = ["契約ID"],
                                right_on = ["contract_id"],
                                how="left")
        print("merge df_kinto and df_WeblogData:{}, nunique 申込書受付番号_result:{}".format(self.df_kinto.shape, 
                                                                                      self.df_kinto["契約ID"].nunique()))

        ### merge chomonix
        self.df_kinto = pd.merge(self.df_kinto,
                                self.df_ChomonixData,
                                left_on = ["member_id"],
                                right_on = ["メンバーID"],
                                how="left")
        print("merge df_kinto and df_chomonix:{}, nunique 申込書受付番号_result:{}".format(self.df_kinto.shape, 
                                                                                    self.df_kinto["契約ID"].nunique()))

        ### merge contract_license
        self.df_kinto = pd.merge(self.df_kinto,
                            self.df_KINTOLicenceData,
                            left_on = ["契約ID"],
                            right_on = ["契約情報: 申込番号"],
                            how="left")
        print("merge df_kinto and df_KINTOLicenceData:{}, nunique 申込書受付番号_result:{}".format(self.df_kinto.shape, 
                                                                                            self.df_kinto["契約ID"].nunique()))
        
        
    """
    Remove null license number
    """
    def remove_null_licence_reocords(self, ):
        print("df_kinto shape: {}".format(self.df_kinto.shape))
        self.df_kinto = self.df_kinto[~self.df_kinto["運転免許番号"].isnull()]
        print("df_kinto shape: {}".format(self.df_kinto.shape))
        
    """
    Create the order of applications
    """
    def create_order_of_applications(self, ):
        self.df_kinto = self.df_kinto.sort_values(["運転免許番号","与信審査申込日時_datetime"], 
                                                  ascending=[True, True])
        self.df_kinto["審査申込順"] = self.df_kinto.groupby("運転免許番号").cumcount()+1
    
    """
    cleansing pipelines
    """
    def cleansing_pipeline(self,):
        ### preprocess_1
        self.read_data_from_raw()
        self.change_dataframe_dtype()
        self.create_datetime_columns()
        
        ### merge
        self.merge_kinto_data()
        
        ### preprocess_2
        self.remove_null_licence_reocords()
        self.create_order_of_applications()       
        
        return self.df_kinto
    

"""
Cleansing class for merge data
"""
class MergeDataCleaner:
    def __init__(self, df_tfc, df_kinto):
        print(" Start of Cleansing of merge processing data")
        self.df_tfc = df_tfc
        self.df_kinto = df_kinto
        
    """
    Merge tfc and kinto data
    """
    def merge_data(self, ):
        print("df_kinto shape:{}".format(df_kinto.shape))
        print("df_tfc shape:{}".format(df_tfc.shape))
        
        ### kinto merge tfc
        self.df_merge = pd.merge(self.df_tfc,
                                self.df_kinto,
                                left_on = ["免許証番号_license", "一次審査完了順"],
                                right_on = ["運転免許番号", "審査申込順"],
                                how="left")
        # print("merge df_kinto and df_tfc:{}, nunique 契約ID:{}".format(self.df_merge.shape, self.df_merge["契約ID"].nunique()))
        # print("与信結果:{}".format(self.df_merge["与信結果_result"].value_counts()))
        
    """
    Delete unnecessary records
    """
    def delete_unnecessary_records(self, ):
        print("df_merge shape: {}".format(self.df_merge.shape))
        self.df_merge = self.df_merge[~self.df_merge["VEIS_９ランク_result"].isin(['Z(照会ｴﾗｰ)'])]
        print("df_merge shape: {}".format(self.df_merge.shape))
        
    """
    Creating the objective variable
    """
    def calculate_month_diff(self, start_date, end_date):
        if pd.isnull(start_date) or pd.isnull(end_date):
            return None
        else:
            diff = relativedelta(end_date, start_date)
            return diff.years * 12 + diff.months
    def create_objective_variable(self, ):
        ### Calculate the difference in months and store it in a new column
        print("create_objective_variable")
        self.df_merge["diff_month_betw_受入年月_初回４次延滞発生年月"] = self.df_merge.apply(lambda row: self.calculate_month_diff(row["受入年月_object_datetime"], 
                                                                                                                     row['初回４次延滞発生年月_object_datetime']),axis=1)
        self.df_merge["diff_month_betw_受入年月_期失処理日"] = self.df_merge.apply(lambda row: self.calculate_month_diff(row["受入年月_object_datetime"], 
                                                                         row['期失処理日_object_datetime']), axis=1)
        
        ### Creating flags before 12 months
        self.df_merge["within_12months_diff_month_betw_受入年月_初回４次延滞発生年月"] = np.where(self.df_merge["diff_month_betw_受入年月_初回４次延滞発生年月"] < 12, 1, 0)
        self.df_merge["within_12months_diff_month_betw_受入年月_期失処理日"] = np.where(self.df_merge["diff_month_betw_受入年月_期失処理日"] < 12, 1, 0)

        ### Total of flags before 12 months
        self.df_merge["within_12months_CreditLoss"] = np.where((self.df_merge["within_12months_diff_month_betw_受入年月_初回４次延滞発生年月"]==1)| \
                                                          (self.df_merge["within_12months_diff_month_betw_受入年月_期失処理日"]==1), 1, 0)
        
        print("value_counts:{}".format(self.df_merge["within_12months_CreditLoss"].value_counts()))

    
    """
    Cleansing pipelines
    """
    def cleansing_pipeline(self,):
        ### preprocess   
        self.merge_data()
        self.delete_unnecessary_records()
        self.create_objective_variable()
        
        ### save data
        output_csv_for_s3(bucket = config.S3_BUCKET, 
                          key = config.KEY_INDIVISUAL_CLEANSING_TRAINING, 
                          filename = config.FILENAME_CLEANSING_DATA, 
                          df = self.df_merge)
    
        
if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    TFCProcessingDataCleaner = TFCProcessingDataCleaner()
    df_tfc = TFCProcessingDataCleaner.cleansing_pipeline()
    
    KINTOInnerDataCleaner = KINTOInnerDataCleaner()
    df_kinto = KINTOInnerDataCleaner.cleansing_pipeline()
    
    MergeDataCleaner = MergeDataCleaner(df_tfc, df_kinto)
    MergeDataCleaner.cleansing_pipeline()
    