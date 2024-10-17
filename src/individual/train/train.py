import io
import os
import time
import logging
import tarfile

import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from src.common.common_func import load_config
from src.common.common_func import read_csv_from_s3
from src.common.common_func import output_csv_for_s3
from src.common.common_func import query_athena_to_s3
from src.common.common_func import get_latest_file
from src.common.common_func import upload_model_to_s3

import lightgbm as lgb
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score, f1_score,average_precision_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve, auc,log_loss
import optuna

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Train class  
"""
class Train:
    def __init__(self,):
        logger.info("----- Start of train Class -----")
        
    """
    read cv/train/valid/test data
    """
    def read_data_from_preprocessed(self,):
        logger.info("-- start read_data_from_preprocessed() --")
        
        ### cv
        latest_filename_CVData = get_latest_file(bucket = config.S3_BUCKET,
                                                 prefix = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING,
                                                 filter_keyword = "cv")
        
        logger.info("Latest file name of CV data : {} ".format(latest_filename_CVData))
        self.df_cv  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                                                   filename = latest_filename_CVData, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(latest_filename_CVData, self.df_cv.shape))
        
        ### train
        latest_filename_TrainData = get_latest_file(bucket = config.S3_BUCKET,
                                                 prefix = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING,
                                                 filter_keyword = "train")
        
        logger.info("Latest file name of Train data : {} ".format(latest_filename_TrainData))
        self.df_train  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                                                   filename = latest_filename_TrainData, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(latest_filename_TrainData, self.df_train.shape))
        
        ### valid
        latest_filename_ValidData = get_latest_file(bucket = config.S3_BUCKET,
                                                 prefix = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING,
                                                 filter_keyword = "valid")
        
        logger.info("Latest file name of Valid data : {} ".format(latest_filename_ValidData))
        self.df_valid  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                                                   filename = latest_filename_ValidData, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(latest_filename_ValidData, self.df_valid.shape))
        
        ### test
        latest_filename_TestData = get_latest_file(bucket = config.S3_BUCKET,
                                                 prefix = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING,
                                                 filter_keyword = "test")
        
        logger.info("Latest file name of Test data : {} ".format(latest_filename_TestData))
        self.df_test  = read_csv_from_s3(bucket = config.S3_BUCKET, 
                                                   key = config.KEY_INDIVISUAL_PREPROCESSED_TRAINING, 
                                                   filename = latest_filename_TestData, 
                                                   encoding = "utf-8")
        logger.info("{}:{}\n".format(latest_filename_TestData, self.df_test.shape))
        
    """
    Creating the objective variable
    """
    def creating_objective_variable(self, ):
        logger.info("-- start creating_objective_variable() --")
        y_col = "within_12months_CreditLoss"
        
        ### cv
        self.df_X_cv = self.df_cv.drop([y_col] ,axis=1)
        self.df_y_cv = self.df_cv[y_col]
        self.df_X_cv = self.df_X_cv.drop(columns = ["受入年月_object_datetime","与信審査申込日時_datetime"])
        logger.info("df_X_cv shape:{}   df_y_cv:{}".format(self.df_X_cv.shape, self.df_y_cv.shape))

        ### train
        self.df_X_train = self.df_train.drop([y_col] ,axis=1)
        self.df_y_train = self.df_train[y_col]
        self.df_X_train = self.df_X_train.drop(columns = ["受入年月_object_datetime","与信審査申込日時_datetime"])
        logger.info("df_X_train shape:{}   df_y_train:{}".format(self.df_X_train.shape, self.df_y_train.shape))
        
        ### valid
        self.df_X_valid = self.df_valid.drop([y_col] ,axis=1)
        self.df_y_valid = self.df_valid[y_col]
        self.df_X_valid = self.df_X_valid.drop(columns = ["受入年月_object_datetime","与信審査申込日時_datetime"])
        logger.info("df_X_valid shape:{}   df_y_valid:{}".format(self.df_X_valid.shape, self.df_y_valid.shape))
        
        ### test
        self.df_X_test = self.df_test.drop([y_col] ,axis=1)
        self.df_y_test = self.df_test[y_col]
        self.df_X_test = self.df_X_test.drop(columns = ["受入年月_object_datetime","与信審査申込日時_datetime"])
        logger.info("df_X_test shape:{}   df_y_test:{}".format(self.df_X_test.shape, self.df_y_test.shape))
        

    """
    Exploring hyperparameters by optuna
    """
    def objective(self, trial):
        ### ハイパーパラメータの設定
        param = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),
            'num_leaves': trial.suggest_int('num_leaves', 20, 40),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.6, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
            'randam_state':10,
        }

        ### クロスバリデーション用の設定
        kf = KFold(n_splits=10, 
                   shuffle=True, 
                   random_state=42)
        scores = []

        ### 各Foldごとに学習&スコア算出
        for train_index, valid_index in kf.split(self.df_X_cv):
            ### train, eval
            X_train, y_train = self.df_X_cv.iloc[train_index], self.df_y_cv.iloc[train_index]
            X_valid, y_valid = self.df_X_cv.iloc[valid_index], self.df_y_cv.iloc[valid_index]

            ### LightGBM用データセットの作成
            lgb_train = lgb.Dataset(X_train, y_train)
            lgb_valid = lgb.Dataset(X_valid, y_valid, 
                                    reference=lgb_train)

            ### モデルの学習
            model = lgb.train(param, 
                              lgb_train, 
                              valid_sets=[lgb_valid], 
                              callbacks=[lgb.early_stopping(stopping_rounds=10, 
                                                            verbose=False), # True:何回目の学習でスコア最適化か表示する
                               lgb.log_evaluation(0)] # 0:スコアの遷移を表示しない
                             )
            ### 予測
            y_pred = model.predict(X_valid, 
                                   num_iteration=model.best_iteration)

            ### 評価
            score = log_loss(y_valid, y_pred)
            scores.append(score)

        return np.mean(scores)
    
    def exploring_hyperparameters(self, ):
        logger.info("-- start exploring_hyperparameters() --")
        
        ### 最適ハイパーパラメータの探索
        study = optuna.create_study(direction='minimize')
        study.optimize(self.objective, 
                       n_trials=2)

        ### 最適ハイパーパラメータ
        print("Best trial:")
        self.best_trial = study.best_trial
        print("  Value: ", self.best_trial.value)
        print("  Params: ")
        for key, value in self.best_trial.params.items():
            print(f"    {key}: {value}")
            
    """
    Train with best parameters
    """
    def train_with_best_parameters(self, ):
        logger.info("-- start train_with_best_parameters() --")
        
        logger.info("Convert to LightGBM dataset format")
        df_lgb_train = lgb.Dataset(self.df_X_train, self.df_y_train)
        df_lgb_valid = lgb.Dataset(self.df_X_valid, self.df_y_valid, reference=df_lgb_train)
        
        logger.info("Setting Best parameters")
        best_params = self.best_trial.params
        add_params = {
                        'objective': 'binary',
                        'metric': 'binary_logloss',
                        'verbosity': -1,
                        'boosting_type': 'gbdt',
        }
        best_params.update(add_params)
        
        ### モデルの訓練
        self.model = lgb.train(best_params, 
                          df_lgb_train, 
                          valid_sets=[df_lgb_valid], 
                          callbacks=[lgb.early_stopping(stopping_rounds=10, 
                                                        verbose=True), # True:何回目の学習でスコア最適化か表示する
                           lgb.log_evaluation(1)] # 0:スコアの遷移を表示しない
                         )
    
    """
    Saving and compressing model
    """
    def save_and_compress_model(self, ):
        logger.info("-- start save_and_compress_model() --")
        
        logger.info("create model folder")
        os.makedirs(config.LOCAL_PATH_INDIVISUAL_MODELS 
                    ,exist_ok=True)
        
        logger.info("Save the model locally in txt format")
        model_file = f"{config.LOCAL_PATH_INDIVISUAL_MODELS}/model.txt"
        self.model.save_model(model_file)
        
        logger.info("Compressed in tar.gz format")
        with tarfile.open(f"{config.LOCAL_PATH_INDIVISUAL_MODELS}.tar.gz", "w:gz") as tar:
            tar.add(config.LOCAL_PATH_INDIVISUAL_MODELS ,
                    arcname=os.path.basename(config.LOCAL_PATH_INDIVISUAL_MODELS ))
            
        logger.info("Upload model to s3")
        logger.info(config.KEY_INDIVISUAL_MODELS)
        upload_model_to_s3(bucket = config.S3_BUCKET,
                           key = config.KEY_INDIVISUAL_MODELS,
                           file_name = f"{config.LOCAL_PATH_INDIVISUAL_MODELS}.tar.gz")
    
    """
    Train pipelines
    """
    def train_pipeline(self,):   
        self.read_data_from_preprocessed()
        self.creating_objective_variable()
        self.exploring_hyperparameters()
        self.train_with_best_parameters()
        self.save_and_compress_model()

        
if __name__ == "__main__":
    ### 環境の読み込み
    config = load_config()
    print("execute {} env".format(config.ENV))
    print("current time:{}".format(config.CURRENT_TIME))
    
    Train = Train()
    Train.train_pipeline()