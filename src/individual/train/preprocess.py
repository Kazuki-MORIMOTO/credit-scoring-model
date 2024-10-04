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
Feature Enginieering class for 
"""
class FeatureEngineer:
    def __init__(self,):
        print(" Start of feature engineering")
        
        
        
if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))