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
        df = get_csv(g_par.INPUT_TRAIN_DATA)
        print(df)

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

def get_csv(file_data):
    '''
    Parameters
    ----------
    file_data : dictionary

    Returns
    ----------
    df : DataFrame

    '''
    try:
        g_logger.info('********************************************')
        g_logger.info('[get_csv]')
        g_logger.info('********************************************')

        df = file_util.load_csv(g_logger, file_data)

        return df
    except:
        g_logger.error('get_csv で例外が発生しました')
        raise


# ------------------------------------------------------------
# ★★★★★★  実行部分  ★★★★★★
# ------------------------------------------------------------
if __name__ == '__main__':

    # 初期処理、アプリ内で利用するグローバル変数の取得
    g_bt_ymd, g_par, g_logger = conv_util.start_app(g_python_name, g_list)

    # 実行部分
    main()
