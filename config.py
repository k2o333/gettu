from pathlib import Path
from enum import Enum
import os

# 从环境变量读取TuShare API token - 强制从.env文件读取
try:
    from dotenv import load_dotenv
    load_dotenv('/home/quan/testdata/aspipe/app2/.env')  # 明确指定.env文件路径
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    if TUSHARE_TOKEN is None or TUSHARE_TOKEN == '':
        raise ValueError("TUSHARE_TOKEN 未在 .env 文件中找到，请在 /home/quan/testdata/aspipe/app2/.env 中设置正确的token")
except ImportError:
    # 如果dotenv不可用，尝试直接从环境变量读取
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    if TUSHARE_TOKEN is None or TUSHARE_TOKEN == '':
        raise ValueError("需要安装python-dotenv包或设置TUSHARE_TOKEN环境变量")
except Exception as e:
    raise e

# 目录配置
ROOT_DIR = Path('/home/quan/testdata/aspipe/data')
APP_DIR = Path('/home/quan/testdata/aspipe/app2')
DICT_DIR = ROOT_DIR / 'dictionaries'
FINANCIALS_DIR = ROOT_DIR / 'financials'
DAILY_DIR = ROOT_DIR / 'daily'
EVENTS_DIR = ROOT_DIR / 'events'
HOLDERS_DIR = ROOT_DIR / 'holders'
RESEARCH_DIR = ROOT_DIR / 'research'
MARKET_STRUCTURE_DIR = ROOT_DIR / 'market_structure'
SNAPSHOTS_DIR = ROOT_DIR / 'snapshots'
METADATA_DB_PATH = ROOT_DIR / 'metadata.db'
AREA_DIR = DICT_DIR  # Add area directory for area dictionary

# 分区粒度配置
class PartitionGranularity(Enum):
    YEAR = "year"
    YEAR_MONTH = "year_month"
    NONE = "none"

# API限制配置
API_LIMITS = {
    # 日线数据接口
    'daily': 500,
    'daily_basic': float('inf'),
    'pro_bar': 500,

    # 财务数据接口（VIP版）
    'income_vip': 30,
    'balancesheet_vip': 30,
    'cashflow_vip': 30,
    'fina_indicator_vip': 30,

    # 技术面因子接口
    'stk_factor': 100,
    'stk_factor_pro': 30,

    # 业绩数据接口
    'forecast': 200,
    'express': 200,
    'forecast_vip': 30,
    'express_vip': 30,

    # 资金流数据接口
    'moneyflow': float('inf'),
    'moneyflow_ths': float('inf'),
    'moneyflow_dc': float('inf'),
    'moneyflow_ind_dc': float('inf'),
    'moneyflow_mkt_dc': 500,
    'moneyflow_cnt_ths': float('inf'),
    'moneyflow_ind_ths': float('inf'),

    # 基础信息数据接口
    'stock_basic': float('inf'),
    'trade_cal': float('inf'),
    'stock_company': 500,
    'stock_st': 100,
    'namechange': float('inf'),
    'new_share': 500,
    'bak_basic': 50,

    # 财务辅助数据接口
    'dividend': 200,
    'disclosure_date': 500,

    # 股东及公司行为数据接口
    'top10_holders': 30,
    'top10_floatholders': 30,
    'pledge_stat': float('inf'),
    'pledge_detail': 500,
    'repurchase': 300,
    'share_float': 500,
    'stk_holdertrade': 200,
    'stk_managers': 200,
    'stk_rewards': 200,

    # 技术分析数据接口
    'cyq_perf': 500,
    'cyq_chips': 500,
    'stk_surv': float('inf'),
    'report_rc': 500,

    # 市场行为数据接口
    'suspend_d': float('inf'),
    'broker_recommend': 200,

    # 主营业务构成（VIP版）
    'fina_mainbz': 30,
    'fina_audit': 30,
    'block_trade': float('inf'),
}

# 统一数据接口配置字典
DATA_INTERFACE_CONFIG = {
    'daily': {
        # 接口定义
        'api_name': 'daily',  # 正确的接口名
        'download_func': 'daily',
        'api_params': {'adj': 'hfq'},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'daily_hfq',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',  # by_stock, by_date, by_period
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('daily', 500)
    },

    'daily_basic': {
        'api_name': 'daily_basic',
        'download_func': 'daily_basic',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'daily_basic',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('daily_basic', 500)
    },

    'moneyflow': {
        'api_name': 'moneyflow',
        'download_func': 'moneyflow',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow', 500)
    },

    'moneyflow_ths': {
        'api_name': 'moneyflow_ths',
        'download_func': 'moneyflow_ths',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_ths',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_ths', 500)
    },

    'moneyflow_dc': {
        'api_name': 'moneyflow_dc',
        'download_func': 'moneyflow_dc',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_dc',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_dc', 500)
    },

    'moneyflow_ind_dc': {
        'api_name': 'moneyflow_ind_dc',
        'download_func': 'moneyflow_ind_dc',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_ind_dc',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_ind_dc', 500)
    },

    'moneyflow_mkt_dc': {
        'api_name': 'moneyflow_mkt_dc',
        'download_func': 'moneyflow_mkt_dc',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_mkt_dc',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['trade_date_int']  # No ts_code_id column, sort only by date
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_mkt_dc', 500)
    },

    'moneyflow_cnt_ths': {
        'api_name': 'moneyflow_cnt_ths',
        'download_func': 'moneyflow_cnt_ths',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_cnt_ths',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_cnt_ths', 500)
    },

    'moneyflow_ind_ths': {
        'api_name': 'moneyflow_ind_ths',
        'download_func': 'moneyflow_ind_ths',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'moneyflow_ind_ths',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('moneyflow_ind_ths', 500)
    },

    'block_trade': {
        'api_name': 'block_trade',
        'download_func': 'block_trade',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False,
            'ann_date': False
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'block_trade',
            'partition_granularity': PartitionGranularity.YEAR_MONTH,
            'partition_field': 'trade_date',
            'sort_fields': ['year_month', 'ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('block_trade', 500)
    },

    'cyq_chips': {
        'api_name': 'cyq_chips',
        'download_func': 'cyq_chips',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': MARKET_STRUCTURE_DIR / 'cyq_chips',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['year', 'ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('cyq_chips', 500)
    },

    'cyq_perf': {
        'api_name': 'cyq_perf',
        'download_func': 'cyq_perf',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': MARKET_STRUCTURE_DIR / 'cyq_perf',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['year', 'ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('cyq_perf', 500)
    },

    'stk_surv': {
        'api_name': 'stk_surv',
        'download_func': 'stk_surv',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': RESEARCH_DIR / 'stk_surv',
            'partition_granularity': PartitionGranularity.YEAR_MONTH,
            'partition_field': 'ann_date',
            'sort_fields': ['year_month', 'ts_code_id', 'ann_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_surv', 500)
    },

    'stk_factor': {
        'api_name': 'stk_factor',
        'download_func': 'stk_factor',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'stk_factor',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_factor', 100)
    },

    'stk_factor_pro': {
        'api_name': 'stk_factor_pro',
        'download_func': 'stk_factor_pro',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DAILY_DIR / 'stk_factor_pro',
            'partition_granularity': PartitionGranularity.YEAR,
            'partition_field': 'trade_date',
            'sort_fields': ['ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_factor_pro', 30)
    },

    'forecast': {
        'api_name': 'forecast',
        'download_func': 'forecast',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'forecast.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id', 'ann_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('forecast', 200)
    },

    'express': {
        'api_name': 'express',
        'download_func': 'express',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'express.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id', 'ann_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('express', 200)
    },

    'top10_holders': {
        'api_name': 'top10_holders',
        'download_func': 'top10_holders',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'top10_holders.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('top10_holders', 30)
    },

    'top10_floatholders': {
        'api_name': 'top10_floatholders',
        'download_func': 'top10_floatholders',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'top10_floatholders.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('top10_floatholders', 30)
    },

    'pledge_stat': {
        'api_name': 'pledge_stat',
        'download_func': 'pledge_stat',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': False
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'pledge_stat.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'end_date',
            'sort_fields': ['ts_code_id', 'end_date_int'] if 'end_date_int' in ['ts_code_id', 'end_date_int'] else ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('pledge_stat', 500)
    },

    'repurchase': {
        'api_name': 'repurchase',
        'download_func': 'repurchase',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'repurchase.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('repurchase', 300)
    },

    'share_float': {
        'api_name': 'share_float',
        'download_func': 'share_float',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'share_float.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('share_float', 500)
    },

    'stk_holdertrade': {
        'api_name': 'stk_holdertrade',
        'download_func': 'stk_holdertrade',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'stk_holdertrade.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_holdertrade', 200)
    },

    'stk_managers': {
        'api_name': 'stk_managers',
        'download_func': 'stk_managers',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'stk_managers.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id', 'ann_date_int'] if 'ann_date_int' in ['ts_code_id', 'ann_date_int'] else ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_managers', 200)
    },

    'stk_rewards': {
        'api_name': 'stk_rewards',
        'download_func': 'stk_rewards',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': HOLDERS_DIR / 'stk_rewards.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',
            'batch_strategy': 'by_stock',
            'batch_size': 50
        },

        # API限制
        'api_limit': API_LIMITS.get('stk_rewards', 200)
    },

    'income': {
        'api_name': 'income',
        'download_func': 'income',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': True
        },

        # 存储配置
        'storage': {
            'path': FINANCIALS_DIR / 'income.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'end_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_period',
            'batch_size': 30
        },

        # API限制
        'api_limit': API_LIMITS.get('income', 30)
    },

    'balancesheet': {
        'api_name': 'balancesheet',
        'download_func': 'balancesheet',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': True
        },

        # 存储配置
        'storage': {
            'path': FINANCIALS_DIR / 'balancesheet.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'end_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_period',
            'batch_size': 30
        },

        # API限制
        'api_limit': API_LIMITS.get('balancesheet', 30)
    },

    'cashflow': {
        'api_name': 'cashflow',
        'download_func': 'cashflow',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': True
        },

        # 存储配置
        'storage': {
            'path': FINANCIALS_DIR / 'cashflow.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'end_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_period',
            'batch_size': 30
        },

        # API限制
        'api_limit': API_LIMITS.get('cashflow', 30)
    },

    'fina_indicator': {
        'api_name': 'fina_indicator',
        'download_func': 'fina_indicator',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': True,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': FINANCIALS_DIR / 'fina_indicator.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_period',
            'batch_size': 30
        },

        # API限制
        'api_limit': API_LIMITS.get('fina_indicator', 30)
    },

    'stock_company': {
        'api_name': 'stock_company',
        'download_func': 'stock_company',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'exchange': True
        },

        # 存储配置
        'storage': {
            'path': DICT_DIR / 'stock_company.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': None,
            'sort_fields': ['ts_code']
        },

        # 更新配置
        'update': {
            'frequency': 'monthly',
            'batch_strategy': 'by_exchange',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('stock_company', 500)
    },

    'namechange': {
        'api_name': 'namechange',
        'download_func': 'namechange',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DICT_DIR / 'namechange.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': None,
            'sort_fields': ['ts_code']
        },

        # 更新配置
        'update': {
            'frequency': 'monthly',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('namechange', 500)
    },

    'new_share': {
        'api_name': 'new_share',
        'download_func': 'new_share',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False
        },

        # 存储配置
        'storage': {
            'path': DICT_DIR / 'new_share.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': None,
            'sort_fields': ['ts_code']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('new_share', 500)
    },

    'dividend': {
        'api_name': 'dividend',
        'download_func': 'dividend',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': True,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'dividend.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('dividend', 200)
    },

    'disclosure_date': {
        'api_name': 'disclosure_date',
        'download_func': 'disclosure_date',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': True,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'disclosure_date.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'ann_date',
            'sort_fields': ['ts_code_id', 'ann_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_period',
            'batch_size': 30
        },

        # API限制
        'api_limit': API_LIMITS.get('disclosure_date', 500)
    },



    'suspend_d': {
        'api_name': 'suspend_d',
        'download_func': 'suspend_d',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': True,
            'period': False,
            'exchange': True
        },

        # 存储配置
        'storage': {
            'path': EVENTS_DIR / 'suspend_d',
            'partition_granularity': PartitionGranularity.YEAR_MONTH,
            'partition_field': 'trade_date',
            'sort_fields': ['year_month', 'ts_code_id', 'trade_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('suspend_d', 500)
    },

    'broker_recommend': {
        'api_name': 'broker_recommend',
        'download_func': 'broker_recommend',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'month': True
        },

        # 存储配置
        'storage': {
            'path': RESEARCH_DIR / 'broker_recommend.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': 'month',
            'sort_fields': ['month', 'broker', 'ts_code_id'] if 'month' in ['month', 'broker', 'ts_code_id'] else ['ts_code_id']
        },

        # 更新配置
        'update': {
            'frequency': 'monthly',
            'batch_strategy': 'by_month',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('broker_recommend', 200)
    },

    'report_rc': {
        'api_name': 'report_rc',
        'download_func': 'report_rc',
        'api_params': {},

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'ann_date': True
        },

        # 存储配置
        'storage': {
            'path': RESEARCH_DIR / 'report_rc',
            'partition_granularity': PartitionGranularity.YEAR_MONTH,
            'partition_field': 'ann_date',
            'sort_fields': ['year_month', 'ts_code_id', 'ann_date_int']
        },

        # 更新配置
        'update': {
            'frequency': 'daily',
            'batch_strategy': 'by_date',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('report_rc', 500),

        # 每日请求限制（特别为report_rc添加）
        'daily_limit': 10,  # 每天最多请求10次
        'max_records_per_request': 3000  # 单次最大3000条
    },
    
    'stock_basic': {
        'api_name': 'stock_basic',
        'download_func': 'stock_basic',
        'api_params': {'list_status': 'L'},  # 上市状态

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': False,
            'end_date': False,
            'trade_date': False,
            'period': False,
            'exchange': True,
            'list_status': True
        },

        # 存储配置
        'storage': {
            'path': DICT_DIR / 'stock_basic_dict.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': None,
            'sort_fields': ['ts_code']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',  # 基础信息更新频率较低
            'batch_strategy': 'by_exchange',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('stock_basic', 500)
    },
    
    'trade_cal': {
        'api_name': 'trade_cal',
        'download_func': 'trade_cal',
        'api_params': {'exchange': ''},  # 所有交易所

        # 参数支持能力
        'supports': {
            'ts_code': False,
            'start_date': True,
            'end_date': True,
            'trade_date': False,
            'period': False,
            'exchange': True
        },

        # 存储配置
        'storage': {
            'path': DICT_DIR / 'trade_calendar.parquet',
            'partition_granularity': PartitionGranularity.NONE,
            'partition_field': None,
            'sort_fields': ['cal_date']
        },

        # 更新配置
        'update': {
            'frequency': 'weekly',
            'batch_strategy': 'by_date_range',
            'batch_size': 100
        },

        # API限制
        'api_limit': API_LIMITS.get('trade_cal', 500)
    }
}

# 字段名称标准化映射
FIELD_MAPPING_CONFIG = {
    'trade_date': ['trade_date', 'date', 'tradedate'],
    'ann_date': ['ann_date', 'announcement_date', 'date', 'ann_dt'],
    'ts_code': ['ts_code', 'stock_code', 'code'],
    # 自动映射处理
}

# 数据类型配置
FIELD_TYPE_CONFIG = {
    'date_fields': ['trade_date', 'ann_date', 'end_date', 'report_date'],
    'id_fields': ['ts_code', 'ts_code_id', 'stock_id'],
    'numeric_fields': ['open', 'close', 'high', 'low', 'volume', 'amount'],
    'string_fields': ['name', 'industry', 'area']
}

# API配置
API_RATE_LIMIT = 400  # 默认API限频（每分钟），设置为400以确保安全
API_MAX_RETRIES = 3   # API调用最大重试次数

# 并行处理配置（优化：充分利用28核）
MAX_WORKERS = 28      # 充分利用28核CPU
SHARD_SIZE_STOCKS = 200  # 股票分片大小（优化：减少进程切换开销）
SHARD_SIZE_DATES = 30   # 日期分片大小
SHARD_SIZE_PERIODS = 10 # 报告期分片大小

# 存储配置
COMPRESSION_TYPE = 'zstd'  # 压缩算法

# 内存优化配置（优化：更充分利用32GB内存）
STREAMING_THRESHOLD = 5_000_000  # 流式处理阈值（行数），更充分利用32GB内存
CHUNK_SIZE = 50000             # 分块处理大小


def get_dynamic_streaming_threshold():
    """根据当前内存使用情况动态调整流式处理阈值"""
    import psutil
    memory_percent = psutil.virtual_memory().percent

    if memory_percent > 85:
        # 内存紧张，使用保守阈值
        return 1_000_000
    else:
        # 内存宽松，使用较大阈值以减少调度开销
        return STREAMING_THRESHOLD