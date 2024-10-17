import io
import os
import time
import logging

import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3
from src.common.common_func import get_latest_file

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Feature Enginieering class for 
"""
class FeatureEngineer:
    def __init__(self,):
        logger.info("----- Start of feature engineering Class -----")
    
    """
    read Cleansing Data
    """
    def read_data_from_cleansing(self,):
        logger.info("-- start read_data_from_cleansing() --")
        ### Cleansing Data
        latest_filename_CleansingData = get_latest_file(bucket = config.S3_BUCKET,
                                           prefix = config.KEY_INDIVISUAL_CLEANSING_TRAINING)
        
        logger.info("Latest file name of Application data : {} ".format(latest_filename_CleansingData))
        
        self.df_CleansingData_origin  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_CLEANSING_TRAINING, 
                                                   filename = latest_filename_CleansingData, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(latest_filename_CleansingData, self.df_CleansingData_origin.shape))
        
        ### TFC orginal columns
        self.df_TFC_original_columns  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_RAW_TFCDATA, 
                                                   filename = config.FILENAME_INDIVISUAL_RAW_ORIGINAL_COLUMNS, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(config.FILENAME_INDIVISUAL_RAW_ORIGINAL_COLUMNS,  self.df_TFC_original_columns.shape))
        
    """
    Filter credit results
    """
    def filter_credit_results(self,):
        logger.info("-- start filter_credit_results --")
        self.df_CleansingData = self.df_CleansingData_origin.query("与信結果_result in ['1承認']")
        self.df_CleansingData_reject = self.df_CleansingData_origin.query("与信結果_result in ['2否決']")
        
        logger.info("df shape : {}".format(self.df_CleansingData .shape))
        logger.info("df_reject shape : {}".format(self.df_CleansingData_reject.shape))
        
    """
    Delete leak(including TFC-specific information) column
    """
    def delete_leak_columns(self,):
        logger.info("-- start delete_leak_columns() --")
        
        ### tfc result leak columns
        list_leak_result_columns = [col for col in self.df_CleansingData.columns if "result" in col]
        list_leak_result_columns = list_leak_result_columns + ["VEIS_９ランク_result", 
                                                                "与信結果_result",
                                                                "VEIS_PD_result", 
                                                                "PKSHA_９ランク_result",
                                                                "PKSHA_PD_result",
                                                                "本人＿合成ＲＫ_result",
                                                                "within_12months_diff_month_betw_受入年月_初回４次延滞発生年月",
                                                                "within_12months_diff_month_betw_受入年月_期失処理日",
                                                               ]
        
        ### tfc object leak columns
        list_leak_object_columns = [col for col in self.df_CleansingData.columns if "object" in col]
        list_leak_object_columns = [col for col in list_leak_object_columns if "受入年月_object_datetime" not in col]
        
        ### other leak columns
        list_leak_other_columns = ["diff_month_betw_受入年月_初回４次延滞発生年月", 
                                     "diff_month_betw_受入年月_期失処理日",
                                     "初回４次延滞発生年月_object_datetime",
                                      "契約ステータスSV",
                                      "免許証番号_license",
                                      "契約情報: 申込番号",
                                       "リース契約番号",
                                       "審査申込順",
                                      "契約ステータス",
                                      "車台番号",
                                      "与信結果確定日",
                                      "会員番号",
                                      "本契約申込日時(契約日)",
                                      "一次審査完了年月日時間_datetime",
                                      "申込書受付番号_license",
                                      "契約ID","contract_id",
                                      "一次審査完了順",
                                      # "与信審査申込日時_datetime",
                                      "会員ステータス",
                                      "契約管理番号(TFC設定項目)",
                                      "登録住所",
                                      "型式別車両本体価格（税抜）",
                                      "登録住所",
                                      "与信審査申込日時",
                                      "申込規約同意日時",
                                      "認定型式",
                                      "パッケージ別月額加算額ID",
                                      "承認結果",
                                       "中途解約ステータスSV",
                                       "中途解約",
                                       "契約ID.1",
                                       "グレード説明文",
                                       "外板色ID",
                                       "運転免許番号",
                                       "グレード名",
                                       "パッケージ説明文",
                                       "内装色ID",
                                       "外板色コード",
                                       "パッケージ_グレードコード",
                                       "郵便番号",
                                       "内装色コード",
                                       "visitorid",
                                       # "メンバーID",
                                       "member_id",
                                       "パッケージコード",
                                       "メンバーID_x",
                                       # "メールアドレス",
                                      ]
        ### duplicates columns
        list_duplicates_columns = [col for col in self.df_CleansingData.columns if col.endswith('_y')]
        
        ### tfc　specific　columns
        
        
        ### drop leak columns
        self.df_CleansingData = self.df_CleansingData.drop(columns = list_leak_other_columns+\
                                                           list_leak_object_columns+\
                                                           list_leak_result_columns+\
                                                           list_duplicates_columns)
        self.df_CleansingData_reject = self.df_CleansingData_reject.drop(columns = list_leak_other_columns+\
                                                           list_leak_object_columns+\
                                                           list_leak_result_columns+\
                                                           list_duplicates_columns)
        
        logger.info("df_CleansingData shape :{}".format(self.df_CleansingData.shape))
        logger.info("df_CleansingData_reject shape :{}".format(self.df_CleansingData_reject.shape))
        
    """
    Delete tfc original columns
    """
    def delete_tfc_original_columns(self,):
        logger.info("-- Delete tfc original columns --")
        ### TFC固有情報カラム
        list_tfc_columns = self.df_TFC_original_columns["カラム名.1"]+"_explain"

        ### TFC固有情報カラムの削除
        self.df_CleansingData = self.df_CleansingData.drop(columns = list_tfc_columns)
        self.df_CleansingData_reject = self.df_CleansingData_reject.drop(columns = list_tfc_columns)
        
        logger.info("df_CleansingData shape :{}".format(self.df_CleansingData.shape))
        logger.info("df_CleansingData_reject shape :{}".format(self.df_CleansingData_reject.shape))
        
    """
    Label encoding
    """
    def transform_with_unknown(self, encoder, column):
        known_classes = set(encoder.classes_)
        return column.map(lambda x: encoder.transform([x])[0] if x in known_classes else -1)
    
    def label_encoding(self,):
        logger.info("-- Label encoding --")
        
        ### Select object and categorical columns
        list_cat_columns = self.df_CleansingData.select_dtypes(include=['object', 'category']).columns
        list_cat_columns = list_cat_columns.drop(['与信審査申込日時_datetime','受入年月_object_datetime'])
        
        ### Dict for storing encoder
        encoders = {}
        
        ### encode
        logger.info("start df encoding ")
        for col in list_cat_columns:
            ## convert to str
            self.df_CleansingData[col] = self.df_CleansingData[col].astype(str)
            self.df_CleansingData_reject[col] = self.df_CleansingData_reject[col].astype(str)
            
            encoder = LabelEncoder()
            self.df_CleansingData[col] = encoder.fit_transform(self.df_CleansingData[col])
            encoders[col] = encoder

            self.df_CleansingData_reject[col] = self.transform_with_unknown(encoders[col], 
                                                                            self.df_CleansingData_reject[col])
            
    """
    Create cv,train,valid,test data
    """
    def create_train_valid_test(self, ):
        logger.info("-- Create train/valid/test data --")
        logger.info("df_CleansingData shape :{}".format(self.df_CleansingData.shape))
        logger.info("df_CleansingData_reject shape :{}".format(self.df_CleansingData_reject.shape))
        
        ### cv
        self.df_cv = self.df_CleansingData[((self.df_CleansingData["与信審査申込日時_datetime"] >= config.CV_START)&\
                                            (self.df_CleansingData["与信審査申込日時_datetime"] <= config.CV_END))&\
                                           ((self.df_CleansingData["受入年月_object_datetime"] >= config.CV_START)&\
                                            (self.df_CleansingData["受入年月_object_datetime"] <= config.ACCEPT_END))]
        
        logger.info("cv start:{} - end:{}".format(self.df_cv["与信審査申込日時_datetime"].min(), 
                                                  self.df_cv["与信審査申込日時_datetime"].max()))
        logger.info("cv shape : {}".format(self.df_cv.shape))
        logger.info("cv count values:{}".format(self.df_cv["within_12months_CreditLoss"].value_counts()))
        
        ### train
        self.df_train = self.df_CleansingData[((self.df_CleansingData["与信審査申込日時_datetime"] >= config.TRAIN_START)&\
                            (self.df_CleansingData["与信審査申込日時_datetime"] <= config.TRAIN_END))&\
                           ((self.df_CleansingData["受入年月_object_datetime"] >= config.TRAIN_START)&\
                            (self.df_CleansingData["受入年月_object_datetime"] <= config.ACCEPT_END))]
        logger.info("train start:{} - end:{}".format(self.df_train["与信審査申込日時_datetime"].min(), 
                                                  self.df_train["与信審査申込日時_datetime"].max()))
        logger.info("train shape : {}".format(self.df_train.shape))
        logger.info("train count values:{}".format(self.df_train["within_12months_CreditLoss"].value_counts()))
        
        
        ### valid
        self.df_valid = self.df_CleansingData[((self.df_CleansingData["与信審査申込日時_datetime"] > config.TRAIN_END)&\
                       (self.df_CleansingData["与信審査申込日時_datetime"] < config.TEST_START))&\
                      ((self.df_CleansingData["受入年月_object_datetime"] > config.TRAIN_START)&\
                       (self.df_CleansingData["受入年月_object_datetime"] <= config.ACCEPT_END))]
        logger.info("valid start:{} - end:{}".format(self.df_valid["与信審査申込日時_datetime"].min(), 
                                                  self.df_valid["与信審査申込日時_datetime"].max()))
        logger.info("valid shape : {}".format(self.df_valid.shape))
        logger.info("valid count values:{}".format(self.df_valid["within_12months_CreditLoss"].value_counts()))
        
        ### test
        self.df_test = self.df_CleansingData[((self.df_CleansingData["与信審査申込日時_datetime"] >= config.TEST_START)&\
                           (self.df_CleansingData["与信審査申込日時_datetime"] <= config.TEST_END))&\
                          ((self.df_CleansingData["受入年月_object_datetime"] >= config.TEST_START)&\
                           (self.df_CleansingData["受入年月_object_datetime"] <= config.ACCEPT_END))]
        logger.info("test start:{} - end:{}".format(self.df_test["与信審査申込日時_datetime"].min(), 
                                                  self.df_test["与信審査申込日時_datetime"].max()))
        logger.info("test shape : {}".format(self.df_test.shape))
        logger.info("test count values:{}".format(self.df_test["within_12months_CreditLoss"].value_counts()))
        
    
    
    """
    Feature Engineering pipelines
    """
    def feature_engineering_pipeline(self,):   
        self.read_data_from_cleansing()
        self.filter_credit_results()
        self.delete_leak_columns()
        self.delete_tfc_original_columns()
        self.label_encoding()
        self.create_train_valid_test()
        
        ### save data
        output_csv_for_s3(bucket = config.S3_BUCKET, 
                          key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                          filename = config.FILENAME_PREPROCESSED_CV_DATA, 
                          df = self.df_cv)
        output_csv_for_s3(bucket = config.S3_BUCKET, 
                          key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                          filename = config.FILENAME_PREPROCESSED_TRAIN_DATA, 
                          df = self.df_train)
        output_csv_for_s3(bucket = config.S3_BUCKET, 
                          key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                          filename = config.FILENAME_PREPROCESSED_VALID_DATA, 
                          df = self.df_valid)
        output_csv_for_s3(bucket = config.S3_BUCKET, 
                          key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                          filename = config.FILENAME_PREPROCESSED_TEST_DATA, 
                          df = self.df_test)

        
if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    FeatureEngineer = FeatureEngineer()
    FeatureEngineer.feature_engineering_pipeline()