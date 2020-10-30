''' coding: utf-8 '''

JOB_NAME = ''
STEP_NAME = ''
LOGFILE_PATH = '/Users/hidebase/NRI/03_自己研鑽/03_データ分析コンテスト/nri_analysis_contest/300_src/log/'

'''
天気データをロード
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
        'target': str,
    }
}
