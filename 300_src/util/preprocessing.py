# ------------------------------------------------------------
# 処理名  ： データの前処理
# 作成者  ： NRI 碓井
# 作成日  ： 2020.09.08
# ------------------------------------------------------------
# ライブラリのインポート
import time

import pandas as pd

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler, Normalizer, StandardScaler

# ------------------------------------------------------------
# 名義尺度の数値化(決定木分析、ランダムフォレスト)
# ------------------------------------------------------------
def label_encoding(df_category):
    category = df_category.select_dtypes(include='object')
    for col in list(category):
        # LabelEncoderのインスタンス作成
        le = LabelEncoder()
        # ラベルをfit
        le.fit(df_category[col])
        # ラベルを整数に変換
        df_category[col] = le.transform(df_category[col])

    return df_category


# ------------------------------------------------------------
# 名義尺度の数値化(回帰分析)
# ------------------------------------------------------------
def one_hot_encoding(df_category):
    '''
    OneHotEncoderを使った場合
    多重共線性に注意する必要あり
    カテゴリーごとに列の削減を行うべき
    '''

    # 定性データと定量データに分割
    category_data = df_category.select_dtypes(include='object')
    numeric_data = df_category.select_dtypes(exclude='object')

    # OneHotEncoderのインスタンス作成
    ohe = OneHotEncoder(handle_unknown='ignore')
    # ダミー変数化したいデータをfit
    ohe.fit(category_data)
    # ダミー変数に変換して、Numpy配列の形へ
    dummy_data = ohe.transform(category_data).toarray()
    # ダミー変数の列名を作成
    dummies_name = ohe.get_feature_names(category_data.columns)
    # データフレームに変換
    df_dummies = pd.DataFrame(dummy_data, columns=dummies_name)
    # 元データに結合
    df_merge = pd.concat([df_dummies, numeric_data], axis=1)

    return df_merge


# ------------------------------------------------------------
# 名義尺度の数値化(回帰分析・多重共線性を回避)
# ------------------------------------------------------------
def multicollinearity(df_category):
    '''
    学習済みの加工ルールを保存できないため
    学習時と同じ加工処理を予測データに対しても行いたい場合は
    scikit-learnメソッドを使うべき
    '''

    category = df_category.select_dtypes(include='object')
    for col in list(category):
        df_dummy = pd.get_dummies(df_category[col], drop_first=True)
        df_category = pd.concat([df_category.drop([col], axis=1), df_dummy], axis=1)

    return df_category


# ------------------------------------------------------------
# 正規化・標準化
# ------------------------------------------------------------
def stds_norms_mms(df, scaler):
    if scaler == 'mms':
        mms = MinMaxScaler()
        mms.fit_transform(df)
    elif scaler == 'stds':
        stds = StandardScaler()
        stds.fit_transform(df)
    elif scaler == 'norms':
        norms = Normalizer()
        norms.fit_transform(df)
    return df


# ------------------------------------------------------------
# ******  データの前処理  ******
# ------------------------------------------------------------
