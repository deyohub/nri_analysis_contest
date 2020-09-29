'''coding:utf-8'''
# ------------------------------------------------------------
# 処理名  ： パイプライン実行
# 作成者  ： NRI藤田一樹
# 作成日  ： 2020/08/30
# 処理概要： AIモデルパイプライン処理
# ------------------------------------------------------------
# ライブラリのインポート
import os
import sys
import traceback

from util import conv_util

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

        # 特徴量の作成
        os.system('python ../●●.py')

        # 特徴量の作成
        os.system('python ../●●.py')

        # 学習の実行
        os.system('python ../510_predict_lightgbm_model.py')

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
# ★★★★★★  実行部分  ★★★★★★
# ------------------------------------------------------------
if __name__ == '__main__':

    # 初期処理、アプリ内で利用するグローバル変数の取得
    g_bt_ymd, g_par, g_logger = conv_util.start_app(g_python_name, g_list)

    # 実行部分
    main()

# ------------------------------------------------------------
# ★★★★★★  パイプライン実行  ★★★★★★
# ------------------------------------------------------------
