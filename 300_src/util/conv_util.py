''' coding: utf-8 '''
# ------------------------------------------------------------
# 処理名  ： 【共通】プログラム処理系 Util関数
# 作成者  ： NRI 川村
# 作成日  ： 2019.08.02
# ------------------------------------------------------------

# ライブラリのインポート
import datetime
import importlib
import os
import sys
import logging
from logging import FileHandler, Formatter, StreamHandler, getLogger
from socket import gethostname

VAR_LIST_SIZE = 11
IDX_BT_YMD = 0
IDX_PAR = 1
IDX_STR_JOB = 2
IDX_STR_STEP = 3
IDX_LOGGER = 4
IDX_PGM_NAME = 5
IDX_PID = 6
IDX_PARENT_PID = 7
IDX_DT_START = 8
IDX_FLG_LOGGING = 9
IDX_HOST_NAME = 10

LOG_LEVEL = logging.INFO
# LOG_LEVEL = logging.DEBUG

DUMMY_PPID = 'jobid_undefined'


# ------------------------------------------------------------
# 引数情報取得
# ------------------------------------------------------------
def start_app(str_python_name, var_list):
    '''
    バッチアプリ初期化処理

    Parameters
    ----------
    str_python_name : str
        Python プログラム名
    var_list : list
        グローバル変数リスト

    Returns
    ----------
    bt_ymd : str
        バッチ日付
    par : module
        パラメータモジュール
    logger : object
        logger インスタンス
    '''
    try:
        # 起動時間の取得・グローバル変数リストに格納
        var_list[IDX_DT_START] = datetime.datetime.now()

        # ログ出力フラグを設定（出力未済）
        var_list[IDX_FLG_LOGGING] = False

        # プログラム名をグローバル変数リストに格納
        var_list[IDX_PGM_NAME] = str_python_name

        # 起動時の引数からプログラムのバッチ日付、パラメータファイル、ジョブのプロセスIDを取得する
        num_arg = len(sys.argv)
        # print('arg num = ', num_arg)
        if num_arg > 5:
            raise RuntimeError('There are too many arguments')
        if num_arg == 5:
            bt_ymd = sys.argv[1]
            param_file = sys.argv[2]
            str_ppid = sys.argv[3]
            log_file = sys.argv[4]
        elif num_arg == 4:
            bt_ymd = sys.argv[1]
            param_file = sys.argv[2]
            str_ppid = sys.argv[3]
        elif num_arg == 3:
            bt_ymd = sys.argv[1]
            param_file = sys.argv[2]
            str_ppid = DUMMY_PPID
        else:
            raise RuntimeError('There are not enough arguments')

        # 日付、ジョブのプロセスIDをグローバル変数リストに格納
        var_list[IDX_BT_YMD] = bt_ymd
        var_list[IDX_PARENT_PID] = str_ppid

        # PID の取得・グローバル変数リストに格納
        var_list[IDX_PID] = str(os.getpid())

        # パラメータファイルの取得
        # パラメータファイルが無い場合は以下の Exception が発生する。
        par = importlib.import_module('param.' + param_file)

        # パラメータをグローバル変数リストに格納
        var_list[IDX_PAR] = par

        # パラメータから、ジョブ名、ステップ名を取得
        var_list[IDX_STR_JOB] = par.JOB_NAME
        var_list[IDX_STR_STEP] = par.STEP_NAME

        # ホスト名を取得
        var_list[IDX_HOST_NAME] = gethostname()

        # ログディレクトリの存在確認
        if num_arg != 5:
            # log_file_path = os.path.join(par.LOGFILE_PATH, bt_ymd)
            log_file_path = par.LOGFILE_PATH
            log_file = os.path.join(log_file_path,
                                    par.JOB_NAME + '_'
                                    + '_' + var_list[IDX_HOST_NAME]
                                    + '_' + str_ppid + '_py.log')

            if os.path.exists(log_file_path) is False:
                raise FileNotFoundError('There is no log directory')

        # loggerの定義
        logger = getLogger("log")
        logger.setLevel(LOG_LEVEL)

        # ログフォーマット定義
        handler_format \
            = Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # 標準出力へのログ出力
        stream_handler = StreamHandler()
        stream_handler.setFormatter(handler_format)
        stream_handler.setLevel(LOG_LEVEL)

        # ファイルへのログ出力 'a'(append)は追加書き込み、'w'は上書き
        file_handler = FileHandler(log_file, 'a')
        file_handler.setFormatter(handler_format)
        file_handler.setLevel(LOG_LEVEL)

        # loggerとHandlerの紐づけ
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

        # logger をグローバル変数リストに格納
        var_list[IDX_LOGGER] = logger
        # print('var_list =\n %s', var_list)

        # ログヘッダーの出力
        logger.info('============   PYTHON   LOG   ============')
        logger.info('JOB: %s', var_list[IDX_STR_JOB])
        logger.info('STEP: %s', var_list[IDX_STR_STEP])
        logger.info('PGM: %s', var_list[IDX_PGM_NAME])
        logger.info('PID: %s', var_list[IDX_PID])
        logger.info('PARENT_PID: %s', var_list[IDX_PARENT_PID])
        logger.info('HOSTNAME: %s', var_list[IDX_HOST_NAME])
        logger.info('------------------------------------------')
        logger.info('')

        # ログ出力フラグを設定（出力可能）
        var_list[IDX_FLG_LOGGING] = True

        return bt_ymd, par, logger

    except Exception as e:
        print('Error occurred in start_app :', e)
        # raise Exception

        if var_list[IDX_FLG_LOGGING]:
            end_app(var_list)

        sys.exit(1)

    finally:
        pass


def end_app(var_list):
    '''
    バッチアプリ終了処理

    Parameters
    ----------
    var_list : list
        グローバル変数リスト

    Returns
    ----------
    なし
    '''

    # 終了時間の取得
    dt_end = datetime.datetime.now()

    # フッターの出力
    logger = var_list[IDX_LOGGER]
    logger.info('')
    logger.info('------------------------------------------')
    logger.info('JOB: %s', var_list[IDX_STR_JOB])
    logger.info('STEP: %s', var_list[IDX_STR_STEP])
    logger.info('PGM: %s', var_list[IDX_PGM_NAME])
    logger.info('PID: %s', var_list[IDX_PID])
    logger.info('PARENT_PID: %s', var_list[IDX_PARENT_PID])
    logger.info('HOSTNAME: %s', var_list[IDX_HOST_NAME])
    logger.info('ELAPSED: {:.2f}[sec]'.format(
        (dt_end - var_list[IDX_DT_START]).total_seconds()))
    logger.info('============   PYTHON   LOG   ============')
