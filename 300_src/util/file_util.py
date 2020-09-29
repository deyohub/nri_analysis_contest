# ------------------------------------------------------------
# 処理名  ： 【共通】ファイル系 Util関数
# 作成者  ： NRI 清野
# 作成日  ： 2019.06.13
# ------------------------------------------------------------
# ライブラリのインポート
import pickle
import time

import pandas as pd


# ------------------------------------------------------------
# [csv] ファイル読込
# ------------------------------------------------------------
def load_csv(logger, file_data, encode='utf8'):
    '''
    csv形式のファイルをロードし、DataFrameに格納する
    - SEJ基幹とのIF時は、encode='cp932'を設定してロードする
    - AI基盤内は、UTF-8で稼働するので、デフォルトで利用する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    file_data : dictionary
        読込対象のファイルの情報（以下の情報を含む）
        file_dir : str
            読込対象のディレクトリ
        file_name : str
            読込対象のファイル名
        usecols : list
            読込対象のカラム指定
        dtype : dictionary
            カラムごとの型指定
    encode: str
        読込対象のエンコードの指定

    Returns
    ----------
    df : DataFrame
        ロードしたファイルが格納されているDataFrame

    Example
    ----------
    使用方法：
    df = file_util.load_csv(g_logger, g_par.MISE_MASTER, encode='cp932')

    '''

    try:
        # 計測開始
        start = time.time()

        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']
        col = file_data['usecols']
        col_type = file_data['dtype']

        # CSVファイルの読み込み
        # 指定カラム有無で分岐
        logger.info('--[file_util：load_csv]--------------------')
        logger.info('PATH  : ' + path)

        if col is None:
            df = pd.read_csv(path, dtype=col_type, engine='c', memory_map=True,
                             encoding=encode, na_filter=True)
        else:
            df = pd.read_csv(path, usecols=col, dtype=col_type, engine='c',
                             memory_map=True, encoding=encode, na_filter=True)

    except FileNotFoundError:
        logger.error('FileNotFoundError')
        raise

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('SHAPE : ' + str(df.shape))
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

        return df

    finally:
        pass


# ------------------------------------------------------------
# [csv] ファイル書込
# ------------------------------------------------------------
def write_csv(logger, df, file_data, encode='utf8', out_header=True):
    '''
    DataFrameをcsv形式でファイルに出力する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    df : DataFrame
        出力対象のDataFrame
    file_data : dictionary
        出力の情報（以下の情報を含む）
        file_dir : str
            出力先のディレクトリ
        file_name : str
            出力先のファイル名
        outcols : list
            出力カラムの指定
    encode : str
        出力エンコードの指定
    out_header:boolean
        ヘッター部の出力指定

    Returns
    ----------
    None

    Example
    ----------
    使用方法：
    file_util.write_csv(g_logger, df, g_par.DF_AI_MISE_MASTER)

    '''

    try:
        # 計測開始
        start = time.time()
        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']
        col = file_data['outcols']

        # 指定カラム有無で分岐
        logger.info('--[file_util：write_csv]--------------------')
        logger.info('PATH  : ' + path)

        if col is None:
            shape = str(df.shape)
            logger.info('SHAPE : ' + shape)
            df.to_csv(path, encoding=encode, index=False, header=out_header)

        else:
            shape = str(df[col].shape)
            logger.info('SHAPE : ' + shape)
            df.to_csv(path, columns=col, encoding=encode,
                      index=False, header=out_header)

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

    finally:
        pass


# ------------------------------------------------------------
# [pickle型] ファイル読込
# ------------------------------------------------------------
def load_pickle(logger, file_data):
    '''
    pickle型のファイルをロードし、DataFrameに格納する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    file_data : dictionary
        読込対象のファイルの情報（以下の情報を含む）
        file_dir : str
            読込対象のディレクトリ
        file_name : str
            読込対象のファイル名
        usecols : list
            読込対象のカラム指定

    Returns
    ----------
    df : DataFrame
        ロードしたファイルが格納されているDataFrame

    '''
    try:
        # 計測開始
        start = time.time()
        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']
        col = file_data['usecols']

        # 指定カラム有無で分岐
        logger.info('--[file_util：load_pickle]--------------------')
        logger.info('PATH  : ' + path)

        if col is None:
            df = pd.read_pickle(path, compression=None)
        else:
            df = pd.read_pickle(path, compression=None)
            df = df[col]

    except FileNotFoundError:
        logger.error('FileNotFoundError')
        raise

    except Exception:

        raise

    else:
        # ログ出力
        logger.info('SHAPE : ' + str(df.shape))
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

        return df

    finally:
        pass


# ------------------------------------------------------------
# [pickle型] ファイル書込
# ------------------------------------------------------------
def write_pickle(logger, df, file_data):
    '''
    DataFrameをpickle型でファイルに出力する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    df : DataFrame
        出力対象のDataFrame
    file_data : dictionary
        出力の情報（以下の情報を含む）
        file_dir : str
            出力先のディレクトリ
        file_name : str
            出力先のファイル名
        outcols : list
            出力先のカラム指定
        dtype : dictionnary
            出力時の型整理

    Returns
    ----------
    None

    '''
    try:
        # 計測開始
        start = time.time()

        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']
        col = file_data['outcols']
        dtype = file_data['dtype']

        # 出力dfの型整理
        df = df.astype(dtype)

        # indexを整理
        df.reset_index(inplace=True, drop=True)

        # ファイルのオープン
        logger.info('--[file_util：write_pickle]--------------------')
        logger.info('PATH  : ' + path)

        with open(path, "wb") as pickle_file:
            # 指定カラム有無で分岐
            if col is None:
                shape = str(df.shape)
                logger.info('SHAPE : ' + shape)
                pickle.dump(df, pickle_file, protocol=4)
            else:
                shape = str(df[col].shape)
                logger.info('SHAPE : ' + shape)
                pickle.dump(df[col], pickle_file, protocol=4)

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

    finally:
        pass


# ------------------------------------------------------------
# [flat file型] ファイル読込
# ------------------------------------------------------------
def load_flat(logger, file_data, encode='cp932'):
    '''
    flat形式のファイルをロードし、DataFrameに変換する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    file_data : dictionary
        読込対象のファイルの情報（以下の情報を含む）
        path : str
            読込対象のファイルの格納パス
        usecols : list
            読込対象のカラム指定
        colspecs : タプルのリスト形式 例)[(0, 6), (10, 12)]
            カラムのバイト長
        dtype : dictionary
            カラムごとの型指定
    encode : str
        読込対象のエンコード指定

    Returns
    ----------
    df : DataFrame
        ロードしたファイルが格納されているDataFrame

    '''
    try:
        # 計測開始
        start = time.time()

        # ファイル情報を展開
        path = file_data['path']
        usecols = file_data['usecols']
        colspecs = file_data['colspecs']
        col_type = file_data['dtype']

        # ファイル読み込み
        logger.info('--[file_util：load_flat]--------------------')
        logger.info('PATH  : ' + path)

        df_flat = pd.read_fwf(path, names=usecols, colspecs=colspecs,
                              converters=col_type, encoding=encode)

    except FileNotFoundError:
        logger.error('FileNotFoundError')
        raise

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('SHAPE : ' + str(df_flat.shape))
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

        return df_flat

    finally:
        pass


# ------------------------------------------------------------
# [dat] ファイル読込
# ------------------------------------------------------------
def load_dat(logger, file_data, encode='cp932'):
    '''
    dat形式※のファイルをロードし、DataFrameに格納する
    （※区切り文字 '|'、文字コードは SJIS（cp932））

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    file_data : dictionary
        読込対象のファイルの情報（以下の情報を含む）
        file_dir : str
            読込対象のディレクトリ
        file_name : str
            読込対象のファイル名
        filecols : dictionary
            読込対象の全カラム指定
        dtype : dictionary
            カラムごとの型指定
        usecols : list
            読込対象のカラム指定
    encode : str
        読込対象のエンコード指定

    Returns
    ----------
    df : DataFrame
        ロードしたファイルが格納されているDataFrame

    '''
    try:
        # 計測開始
        start = time.time()

        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']
        file_col = file_data['filecols']
        col_type = file_data['dtype']
        col = file_data['usecols']

        # datファイルの読み込み
        logger.info('--[file_util：load_dat]--------------------')
        logger.info('PATH  : ' + path)

        df = pd.read_csv(
            path, names=file_col, dtype=col_type, engine='c', header=None,
            memory_map=True, encoding=encode, sep='|', na_filter=True)

        # 指定カラム有無で分岐
        if col is not None:
            df = df[col]

    except FileNotFoundError:
        logger.error('FileNotFoundError')
        raise

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('SHAPE : ' + str(df.shape))
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

        return df

    finally:
        pass


# ------------------------------------------------------------
# [dat] ファイル書込
# ------------------------------------------------------------
def write_dat(logger, str_data, file_data, encode='cp932'):
    '''
    str型の文字列をファイル出力する

    Parameters
    ----------
    logger : logger object
        ログ出力用のlogger
    str_data : str
        出力対象の文字列
    file_data : dictionary
        出力の情報（以下の情報を含む）
        file_dir : str
            出力先のディレクトリ
        file_name : str
            出力先のファイル名
    encode : str
        出力エンコードの指定

    Returns
    ----------
    None

    '''

    try:
        # 計測開始
        start = time.time()

        # ファイル情報を展開
        path = file_data['file_dir'] + file_data['file_name']

        # ファイル出力
        logger.info('--[file_util：write_dat]--------------------')
        logger.info('PATH  : ' + path)
        with open(path, 'w', encoding=encode) as f:
            f.write(str_data)

    except Exception:
        raise

    else:
        # ログ出力
        logger.info('TIME  : {:.2f}'.format(time.time() - start) + '[sec]')
        logger.info('------------------------------------')
        logger.info('')

    finally:
        pass


# ------------------------------------------------------------
# ★★★★★★  【共通】ファイル系 Util関数  ★★★★★★
# ------------------------------------------------------------
