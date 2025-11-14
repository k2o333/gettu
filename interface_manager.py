import polars as pl
import pandas as pd
import tushare as ts
import logging
from config import DATA_INTERFACE_CONFIG, API_LIMITS, TUSHARE_TOKEN
from rate_limiter import safe_api_call
import datetime
from datetime import datetime, timedelta

# Initialize TuShare with the token from config
try:
    pro = ts.pro_api(TUSHARE_TOKEN)
except Exception as e:
    logging.error(f"初始化TuShare API失败，请检查TUSHARE_TOKEN是否正确配置: {str(e)}")
    raise

class InterfaceManager:
    """接口管理器类，根据配置动态生成下载函数"""
    
    @staticmethod
    def get_download_function(data_type: str):
        """根据配置动态生成下载函数"""
        if data_type not in DATA_INTERFACE_CONFIG:
            raise ValueError(f"不支持的数据类型: {data_type}")
        
        config = DATA_INTERFACE_CONFIG[data_type]
        api_name = config['download_func']
        supports = config['supports']

        def dynamic_download(**kwargs):
            # 构建API参数
            params = config['api_params'].copy()

            # 根据supports配置筛选有效参数
            for param_name, is_supported in supports.items():
                if is_supported and param_name in kwargs:
                    params[param_name] = kwargs[param_name]

            # 执行API调用（现在pro_bar使用daily接口，所以使用通用逻辑）
            return safe_api_call(pro, api_name, **params)

        return dynamic_download


class ParameterBuilder:
    """参数构建器"""
    
    @staticmethod
    def build_params(data_type: str, **user_params):
        """根据配置构建API参数"""
        if data_type not in DATA_INTERFACE_CONFIG:
            raise ValueError(f"不支持的数据类型: {data_type}")
        
        config = DATA_INTERFACE_CONFIG[data_type]
        params = config['api_params'].copy()

        # 验证并添加用户参数
        for param, value in user_params.items():
            if param in config['supports'] and config['supports'][param]:
                params[param] = value

        return params


def download_data_by_config(data_type: str, **kwargs):
    """根据配置下载数据的通用函数"""
    try:
        download_func = InterfaceManager.get_download_function(data_type)
        df = download_func(**kwargs)
        
        if df is not None:
            df_pl = pl.from_pandas(df)
            logging.info(f"{data_type}数据下载完成: {len(df_pl)} 条记录")
            return df_pl
        else:
            logging.info(f"{data_type}数据下载完成: 无数据")
            return None
    except Exception as e:
        logging.exception(f"下载{data_type}数据时出错: {str(e)}")
        raise