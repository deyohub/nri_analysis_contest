# ------------------------------------------------------------
# 処理名  ： 特徴量設計
# 作成者  ： 碓井 秀幸
# 作成日  ： 2020.10.30
# 処理概要： 特徴量設計を行う
# ------------------------------------------------------------
# ライブラリーのインポート
import os
import sys
import traceback
import pandas as pd
import numpy as np

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
        df_xa, df_xb, df_xc, df_xd, df_xe, \
            df_ya, df_yb, df_yc, df_yd, df_ye, \
            df_za, df_zb, df_zc, df_zd, df_ze = get_train()

        post_train(df_xa, 'train_xa')

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

def get_train():
    '''
    Parameters
    ----------
    file_data : dictionary

    Returns
    ----------
    df1 : DataFrame
    df2 : DataFrame
    df3 : DataFrame

    '''
    try:
        g_logger.info('********************************************')
        g_logger.info('[get_csv]')
        g_logger.info('********************************************')

        file_data = g_par.INPUT_TRAIN_DATA
        df_train = file_util.load_csv(g_logger, file_data)

        col_name_nichi = file_data['usecols'][0]
        col_name_mise = df_train.columns[1]

        # 客数をマージ
        file_data = g_par.INPUT_KYAKU_DATA
        df_kyaku = file_util.load_csv(g_logger, file_data)
        df_train = pd.merge(df_train, df_kyaku, how='left',
                            on=[col_name_nichi, col_name_mise])

        # 売上をマージ
        file_data = g_par.INPUT_URIAGE_DATA
        df_kyaku = file_util.load_csv(g_logger, file_data)
        df_train = pd.merge(df_train, df_kyaku, how='left',
                            on=[col_name_nichi, col_name_mise])

        # フード売上をマージ
        file_data = g_par.INPUT_FOOD_URIAGE_DATA
        df_kyaku = file_util.load_csv(g_logger, file_data)
        df_train = pd.merge(df_train, df_kyaku, how='left',
                            on=[col_name_nichi, col_name_mise])

        # 曜日を追加する
        df_train[col_name_nichi] = pd.to_datetime(
            df_train[col_name_nichi], format='%Y%m%d')
        df_train['week'] = df_train['nichi'].dt.strftime('%A')
        df_train = df_train.iloc[:, [0, 7, 1, 2, 4, 5, 6, 3]]

        col_name_mise = df_train.columns[2]
        col_name_item = df_train.columns[3]
        df_train_xa = df_train[(df_train[col_name_mise] == 'X') & (
            df_train[col_name_item] == 'A')].reset_index()
        df_train_xb = df_train[(df_train[col_name_mise] == 'X') & (
            df_train[col_name_item] == 'B')].reset_index()
        df_train_xc = df_train[(df_train[col_name_mise] == 'X') & (
            df_train[col_name_item] == 'C')].reset_index()
        df_train_xd = df_train[(df_train[col_name_mise] == 'X') & (
            df_train[col_name_item] == 'D')].reset_index()
        df_train_xe = df_train[(df_train[col_name_mise] == 'X') & (
            df_train[col_name_item] == 'E')].reset_index()
        df_train_ya = df_train[(df_train[col_name_mise] == 'Y') & (
            df_train[col_name_item] == 'A')].reset_index()
        df_train_yb = df_train[(df_train[col_name_mise] == 'Y') & (
            df_train[col_name_item] == 'B')].reset_index()
        df_train_yc = df_train[(df_train[col_name_mise] == 'Y') & (
            df_train[col_name_item] == 'C')].reset_index()
        df_train_yd = df_train[(df_train[col_name_mise] == 'Y') & (
            df_train[col_name_item] == 'D')].reset_index()
        df_train_ye = df_train[(df_train[col_name_mise] == 'Y') & (
            df_train[col_name_item] == 'E')].reset_index()
        df_train_za = df_train[(df_train[col_name_mise] == 'Z') & (
            df_train[col_name_item] == 'A')].reset_index()
        df_train_zb = df_train[(df_train[col_name_mise] == 'Z') & (
            df_train[col_name_item] == 'B')].reset_index()
        df_train_zc = df_train[(df_train[col_name_mise] == 'Z') & (
            df_train[col_name_item] == 'C')].reset_index()
        df_train_zd = df_train[(df_train[col_name_mise] == 'Z') & (
            df_train[col_name_item] == 'D')].reset_index()
        df_train_ze = df_train[(df_train[col_name_mise] == 'Z') & (
            df_train[col_name_item] == 'E')].reset_index()

        # for i in df_train[col_name_mise].unique():
        #     for j in df_train[col_name_item].unique():
        #         exec(
        #             f"df_train_{i}{j} = df_train[(df_train['{col_name_mise}'] == '{i}') & (df_train['{col_name_item}'] == '{j}')].reset_index()")
        #         # df[i][j] = df_train[(df_train[col_name_mise] == mise) & (
        #         #     df_train[col_name_item] == item)].reset_index()
        #         # print(df[i][j])

        return df_train_xa, df_train_xb, df_train_xc, df_train_xd, df_train_xe, \
            df_train_ya, df_train_yb, df_train_yc, df_train_yd, df_train_ye, \
            df_train_za, df_train_zb, df_train_zc, df_train_zd, df_train_ze
    except:
        g_logger.error('get_csv で例外が発生しました')
        raise


def post_train(df, df_name):
    file_dir = '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/500_output/'
    file_name = df_name
    path = file_dir + file_name + '.csv'
    df.to_csv(path)


# ------------------------------------------------------------
# ★★★★★★  実行部分  ★★★★★★
# ------------------------------------------------------------
if __name__ == '__main__':

    # 初期処理、アプリ内で利用するグローバル変数の取得
    g_bt_ymd, g_par, g_logger = conv_util.start_app(g_python_name, g_list)

    # 実行部分
    main()
