''' coding: utf-8 '''
# ------------------------------------------------------------
# 処理名  ： メイン処理の実行
# 処理概要：
# ------------------------------------------------------------
# ライブラリーのインポート
import os
import sys
import traceback
import datetime
import pickle

import pandas as pd
import numpy as np

# 評価指標
from sklearn.metrics import mean_squared_error  # RMSE用
from sklearn.metrics import mean_squared_log_error  # RMSLE用

# CV
from sklearn.model_selection import KFold

# アルゴリズム
import lightgbm as lgbm

# 可視化用
import matplotlib.pyplot as plt
import seaborn as sns

# utilプログラム
from util import conv_util
from util import file_util

# グローバル変数定義
g_bt_ymd = ''
g_par = None
g_logger = None
g_python_name = os.path.basename(__file__)
g_list = [None] * conv_util.VAR_LIST_SIZE


# ------------------------------------------------------------
# ★★★★★★  メイン処理部分  ★★★★★★
# ------------------------------------------------------------
def main():

    # バッチ処理の記述
    try:

        # 実行時間の取得
        time = str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

        # ディレクトリのセット
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

        # 特徴量データの取得
        df_train = get_feature_data()

        # LightGBMモデルを学習する
        df_feature_importance = train_lightgbm(df_train, time)

        # 特徴量の重要度を出力する
        write_feature_importance(df_feature_importance, time)

    except Exception:
        # エラースタックを出力
        g_logger.error('========================================')
        g_logger.exception(traceback.format_exc())
        g_logger.error('異常終了 ENDED CODE=1')
        sys.exit(1)

    else:
        g_logger.info('========================================')
        g_logger.info('正常終了 ENDED CODE=0')
        sys.exit(0)

    finally:
        conv_util.end_app(g_list)


# ------------------------------------------------------------
# ★★★★★★  処理  ★★★★★★
# ------------------------------------------------------------
def get_feature_data():
    '''

    Parameters
    ----------
    None

    Returns
    ----------
    df_train

    '''
    try:
        g_logger.info('********************************************')
        g_logger.info('[get_feature_data]')
        g_logger.info('********************************************')

        # 特徴量データを取得する
        df_train = file_util.load_csv(g_logger, g_par.INPUT_NIPPAN_DATA)
        print(df_train.shape)

        # GISデータ
        df_gis = file_util.load_csv(g_logger, g_par.INPUT_GIS_DATA)

        # GISデータを結合
        df_train = pd.merge(df_train, df_gis, on=['org_mise'])

        # 店マスタ
        df_mise = file_util.load_csv(g_logger, g_par.INPUT_MISE_MASTER)

        # 店マスタデータを結合
        df_train = pd.merge(df_train, df_mise, on=['org_mise'])

        # 足りないデータをdrop★ここは暫定でお願いします
        df_train.dropna(subset=['inhabitants', 'employees'], inplace=True)
        print(df_train.shape)
        print(df_train.head(10))

        return df_train

    except:
        g_logger.error('get_feature_data で例外が発生しました')
        raise


def train_lightgbm(df_train, time):
    '''

    Parameters
    ----------

    Returns
    ----------
    None

    '''
    try:
        g_logger.info('********************************************')
        g_logger.info('[train_lightgbm]')
        g_logger.info('********************************************')

        # ディレクトリの作成
        os.mkdir('../600_model/' + str(time))

        # 特徴量とターゲットのカラムを抽出
        target = df_train[g_par.TARGET_COL]
        df_train = df_train[g_par.FEATURE_COL]

        # パラメーターセット
        param = {
            'num_leaves': 31,  # 葉の数
            'min_data_in_leaf': 30,  # 葉の最小サンプル数
            'objective': 'regression',  # regression, binary, multiclass,など
            "metric": 'rmse',  # rmse, mape, binary_logloss, softmax
            # 'num_class': 8,  # multiclass場合
            'max_depth': -1,
            'learning_rate': 0.1,
            "min_child_samples": 100,
            "boosting": "gbdt",  # gbdt, dartなど
            "feature_fraction": 0.9,  # colsample_bytreeのこと
            "bagging_freq": 1,
            "bagging_fraction": 0.9,  # subsampleのこと [bagging_freq:1]とセット
            "bagging_seed": 2020,
            "lambda_l1": 0.1,
            "lambda_l2": 0.1,
            "nthread": -1,  # 並列処理のCPU数
            "verbosity": -1,
            "random_state": 2020
        }

        # KFoldクロスバリデーションをセットする
        Fold = 4
        folds = KFold(n_splits=Fold, shuffle=True, random_state=2020)

        # 学習データのout of foldの結果格納用dfのセット
        train_oof = np.zeros(len(df_train))

        # 特徴量の重要度格納用のdfをセット
        df_feature_importance = pd.DataFrame()

        # 学習の最大回数をセット(大きければだいたいOK)
        num_round = 10000

        # KFoldで学習する
        for fold_, (trn_idx, val_idx) in enumerate(
                folds.split(df_train.values, target.values)):

            g_logger.info('====== [fold:{}] ======'.format(fold_ + 1))

            # trainとvalデータの定義
            train_data = lgbm.Dataset(df_train.iloc[trn_idx][g_par.FEATURE_COL],
                                      label=target.iloc[trn_idx],
                                      categorical_feature=g_par.CATEGORICAL_COL)
            val_data = lgbm.Dataset(df_train.iloc[val_idx][g_par.FEATURE_COL],
                                    label=target.iloc[val_idx],
                                    categorical_feature=g_par.CATEGORICAL_COL)

            # 学習処理の実行
            model = lgbm.train(param,  # パラメーターセット
                               train_data,  # trainデータ
                               num_round,  # num_round数
                               valid_sets=[train_data, val_data],  # バリデーション用データセット
                               verbose_eval=20,  # 詳細を表示する間隔
                               early_stopping_rounds=20  # early_stopping数
                               )

            # ===== [トレーニングデータの予測と精度検証] =====
            # 予測処理[トレーニングデータ]
            preds_train = model.predict(df_train.iloc[trn_idx][g_par.FEATURE_COL],  # トレーニングデータセット
                                        num_iteration=model.best_iteration)  # early_stopping結果
            # 日販0以下を0に置換する
            preds_train = np.where(preds_train > 0, preds_train, 0)

            # trainデータの精度評価
            g_logger.info('====== [trainデータの精度評価] ======')
            g_logger.info('train RMSE:{:.3f}'.format(
                np.sqrt(mean_squared_error(target.iloc[trn_idx], preds_train))))
            g_logger.info('train RMSLE:{:.3f}'.format(
                np.sqrt(mean_squared_log_error(target.iloc[trn_idx], preds_train))))

            # ===== [バリデーションデータの予測と精度検証] =====
            # 予測処理[バリデーションデータ]
            preds_train_oof = model.predict(df_train.iloc[val_idx][g_par.FEATURE_COL],
                                            num_iteration=model.best_iteration)  # num_round数
            # 日販0以下を0に置換する
            preds_train_oof = np.where(preds_train_oof > 0, preds_train_oof, 0)

            # Validationデータの精度評価
            g_logger.info('====== [Validationデータの精度評価] ======')
            g_logger.info('Validation RMSE:{:.3f}'.format(
                np.sqrt(mean_squared_error(target.iloc[val_idx], preds_train_oof))))
            g_logger.info('Validation RMSLE:{:.3f}'.format(
                np.sqrt(mean_squared_log_error(target.iloc[val_idx], preds_train_oof))))

            # fold内の予測値を全体で精度検証するために退避する
            train_oof[val_idx] = preds_train_oof

            # =====[学習済みモデルの評価と検証]=====
            # 学習済みモデルの保存
            save_lightgbm_model(model, fold_ + 1, time)

            # =====[特徴量の重要度の記録]=====
            # importanceのセット
            df_feature_importance_fold = pd.DataFrame()
            df_feature_importance_fold['feature'] = g_par.FEATURE_COL
            df_feature_importance_fold['importance'] = \
                model.feature_importance(importance_type='gain')
            df_feature_importance_fold['fold'] = fold_ + 1
            df_feature_importance = \
                pd.concat([df_feature_importance, df_feature_importance_fold], axis=0)

        g_logger.info('====== [oofデータ全体での精度評価] ======')
        g_logger.info('OOF RMSE:{:.3f}'.format(
            np.sqrt(mean_squared_error(target, train_oof))))
        g_logger.info('OOF RMSLE:{:.3f}'.format(
            np.sqrt(mean_squared_log_error(target, train_oof))))

        return df_feature_importance

    except:
        g_logger.error('train_lightgbm で例外が発生しました')
        raise


def write_feature_importance(df_feature_importance, time):
    '''
    特徴量の重要度を

    Parameters
    ----------
    df_feature_importance : DataFrame
        [特徴量の重要度]

    Returns
    ----------
    None

    '''
    try:
        g_logger.info('********************************************')
        g_logger.info('[write_feature_importance]')
        g_logger.info('********************************************')

        # 特徴量重要度からTOP50個を集計する
        cols = (df_feature_importance[['feature', 'importance']]
                .groupby(['feature'])
                .mean()
                .sort_values(by='importance', ascending=False)[:50].index)

        # 特徴量の重要度からTOP50を抽出する
        df_best_features = df_feature_importance.loc[
            df_feature_importance['feature'].isin(cols)]

        # feature_importanceの出力
        plt.figure(figsize=(14, 30))
        sns.barplot(x='importance', y='feature',
                    data=df_best_features.sort_values(by='importance', ascending=False))
        plt.title('LightGBM Features (avg over folds)')
        plt.tight_layout()
        plt.savefig('../600_model/' + str(time) + '/lgbm_importances.png')

    except:
        g_logger.error('write_feature_importance で例外が発生しました')
        raise


def save_lightgbm_model(model, fold, time):
    '''
    学習済みAIモデルを保存する

    Parameters
    ----------
    logger: logger object
        ログ出力用のlogger

    Returns
    ----------
    None
    '''
    try:

        g_logger.info('[save_lightgbm_model]')

        # 学習済みモデルを出力する
        with open('../600_model/' + str(time) + '/lightGBM_model'
                  + str(fold) + '.model', 'wb') as pickle_file:
            pickle.dump(model, pickle_file)

    except:
        g_logger.error('save_lightgbm_model で例外が発生しました')
        raise


# ------------------------------------------------------------
# ★★★★★★  実行部分  ★★★★★★
# ------------------------------------------------------------
if __name__ == '__main__':

    # 初期処理、アプリ内で利用するグローバル変数の取得
    g_bt_ymd, g_par, g_logger = conv_util.start_app(g_python_name, g_list)

    # 実行部分
    main()

# ------------------------------------------------------------
# ★★★★★★  メイン処理の実行  ★★★★★★
# ------------------------------------------------------------
