''' coding: utf-8 '''

JOB_NAME = ''
STEP_NAME = ''
LOGFILE_PATH = '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/300_src/log/'

'''
配布されたデータをロード
'''

INPUT_TRAIN_DATA = {
    'file_dir': '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/200_input/',
    'file_name': 'train.csv',
    'usecols': [
        'nichi',
        'group_mise',
        'group_item',
        'target',
    ],
    'dtype': {
        'nichi': str,
        'group_mise': str,
        'group_item': str,
        'target': 'Int32',
    }
}

INPUT_KYAKU_DATA = {
    'file_dir': '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/200_input/',
    'file_name': 'kyaku.csv',
    'usecols': [
        'nichi',
        'group_mise',
        'kyaku_param',
    ],
    'dtype': {
        'nichi': str,
        'group_mise': str,
        'kyaku_param': float,
    }
}

INPUT_URIAGE_DATA = {
    'file_dir': '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/200_input/',
    'file_name': 'uriage.csv',
    'usecols': [
        'nichi',
        'group_mise',
        'uriage_param',
    ],
    'dtype': {
        'nichi': str,
        'group_mise': str,
        'uriage_param': float,
    }
}

INPUT_FOOD_URIAGE_DATA = {
    'file_dir': '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/200_input/',
    'file_name': 'food_uriage.csv',
    'usecols': [
        'nichi',
        'group_mise',
        'food_uriage_wariai',
    ],
    'dtype': {
        'nichi': str,
        'group_mise': str,
        'food_uriage_wariai': float,
    }
}
