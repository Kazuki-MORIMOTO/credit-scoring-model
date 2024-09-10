import io
import os
import time

from conf.common_func import load_config
from conf.common_func import read_csv_from_s3

"""
Reading TFC processing data
"""
def read_tfc_processing_data():
    ### 【個人】_与信結果データ 
    df_indivisual_result_to202312 = read_csv_from_s3(bucket=config.S3_BUCKET, 
                                                   key=config.KEY_INDIVISUAL_RAW_TRAINING, 
                                                   filename=config.FILENAME_INDIVISUAL_RAW_RESULT_TO202312, 
                                                   encoding="shift-jis")
    print("{}:{}".format(filename, df_indivisual_result_to202312.shape))
    
    return df_indivisual_result_to202312

"""
Reading and saving　Car data
"""

"""
Reading and saving chomonix data
"""

"""
Reading and saving weblog data
"""



if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    
    ### TFC加工データの読み込み
    df = read_tfc_processing_data()
    print(df.shape)