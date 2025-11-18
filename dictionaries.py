import polars as pl
import logging
from datetime import datetime
from pathlib import Path
from config import DICT_DIR, SNAPSHOTS_DIR
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime
import os
from dictionary_management import DictionaryManager

def evolve_global_dictionaries():
    """
    演进全局字典 - 包括股票基础信息和交易日历
    """
    try:
        logging.info("开始演进全局字典...")

        # Initialize dictionary manager
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # 1. 下载并更新股票基本信息
        stock_basic_df = download_data_by_config('stock_basic')
        if stock_basic_df is not None and len(stock_basic_df) > 0:
            # Add stocks to dictionary management system with permanent IDs
            for row in stock_basic_df.iter_rows(named=True):
                ts_code = row['ts_code']
                # Add stock to dictionary with metadata
                stock_id = dict_manager.add_stock(ts_code, {
                    'name': row.get('name', ''),
                    'area': row.get('area', ''),
                    'industry': row.get('industry', ''),
                    'market': row.get('market', ''),
                    'list_date': row.get('list_date', '')
                })

            # Create ID mapping column using permanent IDs
            ts_code_to_id = {}
            for row in stock_basic_df.iter_rows(named=True):
                ts_code = row['ts_code']
                ts_code_to_id[ts_code] = dict_manager.get_stock_id(ts_code)

            stock_basic_df = stock_basic_df.with_columns([
                pl.col('ts_code').cast(pl.Categorical).alias('ts_code_cat'),
                # Use permanent IDs from dictionary management system
                pl.col('ts_code').map_elements(
                    lambda x: ts_code_to_id.get(x, None),
                    return_dtype=pl.Int64
                ).alias('ts_code_id')
            ])

            # 确保字典目录存在
            DICT_DIR.mkdir(parents=True, exist_ok=True)

            # 保存股票基础信息
            dict_path = DICT_DIR / 'stock_basic_dict.parquet'
            stock_basic_df.write_parquet(dict_path)
            logging.info(f"股票基础字典更新完成: {len(stock_basic_df)} 条记录")

        # 2. 下载并更新交易日历
        trade_cal_df = download_data_by_config('trade_cal')
        if trade_cal_df is not None and len(trade_cal_df) > 0:
            # 确保交易日历目录存在
            DICT_DIR.mkdir(parents=True, exist_ok=True)

            # 保存交易日历
            cal_path = DICT_DIR / 'trade_calendar.parquet'
            trade_cal_df.write_parquet(cal_path)
            logging.info(f"交易日历字典更新完成: {len(trade_cal_df)} 条记录")

    except Exception as e:
        logging.error(f"演进全局字典失败: {str(e)}")
        raise


def evolve_industry_dictionaries():
    """
    演进行业字典
    """
    try:
        logging.info("开始演进行业字典...")

        # Initialize dictionary manager
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # 从股票基础信息中提取行业信息
        stock_basic_path = DICT_DIR / 'stock_basic_dict.parquet'
        if stock_basic_path.exists():
            stock_basic_df = pl.read_parquet(stock_basic_path)

            # 提取行业列表
            industry_df = (stock_basic_df
                          .select(['industry'])
                          .unique()
                          .filter(pl.col('industry').is_not_null()))

            # Add industries to dictionary management system with permanent IDs
            industry_to_id = {}
            for row in industry_df.iter_rows(named=True):
                industry_code = row['industry']
                if industry_code:
                    # Add industry to dictionary with permanent ID
                    industry_id = dict_manager.add_industry(industry_code)
                    industry_to_id[industry_code] = industry_id

            # Create industry dictionary with permanent IDs
            industry_dict_df = pl.DataFrame({
                'industry': list(industry_to_id.keys()),
                'industry_id': list(industry_to_id.values())
            }).with_columns([
                pl.col('industry').cast(pl.Categorical).alias('industry_cat')
            ])

            # 保存行业字典
            industry_path = DICT_DIR / 'industry_dict.parquet'
            industry_dict_df.write_parquet(industry_path)
            logging.info(f"行业字典更新完成: {len(industry_dict_df)} 个行业")

            # 创建股票-行业映射 using permanent IDs
            stock_industry_df = stock_basic_df.with_columns([
                pl.col('industry').map_elements(
                    lambda x: industry_to_id.get(x, None) if x else None,
                    return_dtype=pl.Int64
                ).alias('industry_id')
            ])

            # 保存增强的股票基础信息（包含行业ID）
            enhanced_path = DICT_DIR / 'stock_basic_enhanced.parquet'
            stock_industry_df.write_parquet(enhanced_path)
            logging.info(f"增强股票基础信息保存完成: {len(stock_industry_df)} 条记录")

    except Exception as e:
        logging.error(f"演进行业字典失败: {str(e)}")
        raise


def evolve_area_dictionaries():
    """
    演进地区字典
    """
    try:
        logging.info("开始演进地区字典...")

        # Initialize dictionary manager
        dict_manager = DictionaryManager()
        dict_manager.initialize()

        # 从股票基础信息中提取地区信息
        stock_basic_path = DICT_DIR / 'stock_basic_dict.parquet'
        if stock_basic_path.exists():
            stock_basic_df = pl.read_parquet(stock_basic_path)

            # 提取地区列表
            area_df = (stock_basic_df
                      .select(['area'])
                      .unique()
                      .filter(pl.col('area').is_not_null()))

            # Add regions to dictionary management system with permanent IDs
            area_to_id = {}
            for row in area_df.iter_rows(named=True):
                area_code = row['area']
                if area_code:
                    # Add region to dictionary with permanent ID
                    area_id = dict_manager.add_region(area_code)
                    area_to_id[area_code] = area_id

            # Create area dictionary with permanent IDs
            area_dict_df = pl.DataFrame({
                'area': list(area_to_id.keys()),
                'area_id': list(area_to_id.values())
            }).with_columns([
                pl.col('area').cast(pl.Categorical).alias('area_cat')
            ])

            # 保存地区字典
            area_path = DICT_DIR / 'area_dict.parquet'
            area_dict_df.write_parquet(area_path)
            logging.info(f"地区字典更新完成: {len(area_dict_df)} 个地区")

    except Exception as e:
        logging.error(f"演进地区字典失败: {str(e)}")
        raise


def update_stock_basic_with_ids():
    """
    使用字典为股票基本信息添加ID字段
    """
    try:
        logging.info("开始更新股票基本信息ID...")
        
        # 读取股票基础信息
        stock_basic_path = DICT_DIR / 'stock_basic_dict.parquet'
        if not stock_basic_path.exists():
            logging.warning("股票基础信息文件不存在，跳过ID更新")
            return
            
        stock_basic_df = pl.read_parquet(stock_basic_path)
        
        # 创建ID映射
        stock_basic_df = stock_basic_df.with_columns([
            # 生成数值ID (使用哈希值)
            (pl.col('ts_code').hash() % 2147483647).alias('ts_code_id')
        ])
        
        # 确保快照目录存在
        SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        
        # 保存增强的股票基础信息
        enhanced_path = SNAPSHOTS_DIR / 'stock_basic_enhanced.parquet'
        stock_basic_df.write_parquet(enhanced_path)
        logging.info(f"股票基本信息ID更新完成: {len(stock_basic_df)} 条记录")
        
    except Exception as e:
        logging.error(f"更新股票基本信息ID失败: {str(e)}")
        raise


def get_stock_list():
    """
    获取股票列表
    """
    try:
        # 尝试从增强的股票基础信息中获取
        enhanced_path = SNAPSHOTS_DIR / 'stock_basic_enhanced.parquet'
        if enhanced_path.exists():
            df = pl.read_parquet(enhanced_path)
            return df['ts_code'].to_list()
        
        # 回退到字典中的股票基础信息
        dict_path = DICT_DIR / 'stock_basic_dict.parquet'
        if dict_path.exists():
            df = pl.read_parquet(dict_path)
            return df['ts_code'].to_list()
        
        logging.warning("无法获取股票列表")
        return []
        
    except Exception as e:
        logging.error(f"获取股票列表失败: {str(e)}")
        return []


def get_business_date_range(start_date_str, end_date_str):
    """
    获取两个日期之间的所有交易日
    """
    try:
        # 读取交易日历
        trade_cal_path = DICT_DIR / 'trade_calendar.parquet'
        if trade_cal_path.exists():
            trade_cal_df = pl.read_parquet(trade_cal_path)
            # 过滤出交易日
            business_days = trade_cal_df.filter(
                (pl.col('cal_date') >= start_date_str) &
                (pl.col('cal_date') <= end_date_str) &
                (pl.col('is_open') == 1)
            ).select('cal_date').to_series().to_list()
            return business_days
        else:
            # 如果没有交易日历，返回所有日期
            from datetime import datetime, timedelta
            start_date = datetime.strptime(start_date_str, '%Y%m%d')
            end_date = datetime.strptime(end_date_str, '%Y%m%d')
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            return date_list
    except Exception as e:
        logging.exception(f"获取交易日历失败: {str(e)}")
        # 回退方案：返回所有日期
        from datetime import datetime, timedelta
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        return date_list


def get_ts_code_dict_path():
    """
    获取ts_code字典路径
    """
    # 优先返回增强版的股票基础信息，因为它包含了ID映射
    enhanced_path = SNAPSHOTS_DIR / 'stock_basic_enhanced.parquet'
    if enhanced_path.exists():
        return enhanced_path
    
    # 否则返回字典中的股票基础信息
    dict_path = DICT_DIR / 'stock_basic_dict.parquet'
    return dict_path


def dictionaries_exist():
    """检查字典文件是否已存在"""
    required_files = [
        DICT_DIR / 'stock_basic_dict.parquet',
        DICT_DIR / 'industry_dict.parquet',
        DICT_DIR / 'area_dict.parquet'
    ]

    return all(file_path.exists() for file_path in required_files)


def create_dictionaries(force_recreate=False):
    """
    创建所有字典的主函数 - 修复版本
    """
    try:
        logging.info("开始创建字典...")

        # 创建字典目录
        DICT_DIR.mkdir(parents=True, exist_ok=True)
        SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

        # 检查字典是否已存在
        if not force_recreate and dictionaries_exist():
            logging.info("字典已存在，跳过创建步骤")
            return

        # 演进全局字典
        evolve_global_dictionaries()

        # 演进行业字典
        evolve_industry_dictionaries()

        # 演进地区字典
        evolve_area_dictionaries()

        # 更新股票基本信息ID
        update_stock_basic_with_ids()

        logging.info("字典创建完成")

    except Exception as e:
        logging.error(f"创建字典失败: {str(e)}")
        raise