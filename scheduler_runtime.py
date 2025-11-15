import polars as pl
import logging
from datetime import datetime, timedelta
from config import DATA_INTERFACE_CONFIG, DAILY_DIR, EVENTS_DIR, FINANCIALS_DIR, HOLDERS_DIR, RESEARCH_DIR, MARKET_STRUCTURE_DIR, DICT_DIR
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime
from metadata import update_last_update_date, get_last_update_date
from dictionaries import evolve_global_dictionaries, evolve_industry_dictionaries, evolve_area_dictionaries, update_stock_basic_with_ids, get_business_date_range
import os
from pathlib import Path

class SchedulerRuntime:
    """配置驱动调度器"""
    
    @staticmethod
    def run_daily_update():
        """根据配置执行每日更新，智能检测缺失数据"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        # 遍历所有需要每日更新的接口
        for data_type, config in DATA_INTERFACE_CONFIG.items():
            if config['update']['frequency'] == 'daily':
                try:
                    # 智能检测：查找第一个缺失的日期
                    missing_date = SchedulerRuntime._find_first_missing_date(data_type)
                    if missing_date is None:
                        # 如果没有缺失的日期，使用默认昨天日期
                        missing_date = yesterday
                    
                    # 动态下载数据
                    df = download_data_by_config(data_type, trade_date=missing_date)

                    if df is not None and len(df) > 0:
                        # 动态处理ETL
                        EtlRuntime.process_data(data_type, df=df)

                        # 更新元数据
                        update_last_update_date(data_type, missing_date)
                        
                        logging.info(f"成功更新{data_type}数据: {len(df)}条记录, 日期: {missing_date}")
                    else:
                        logging.info(f"{data_type}在{missing_date}无数据")
                except Exception as e:
                    logging.warning(f"更新{data_type}失败: {str(e)}")

    @staticmethod
    def run_initial_build(data_types=None):
        """根据配置执行初始构建"""
        types_to_build = data_types or DATA_INTERFACE_CONFIG.keys()

        for data_type in types_to_build:
            if data_type not in DATA_INTERFACE_CONFIG:
                continue
                
            config = DATA_INTERFACE_CONFIG[data_type]

            # 根据batch_strategy执行不同的构建策略
            strategy = config['update']['batch_strategy']
            if strategy == 'by_stock':
                SchedulerRuntime._build_by_stock(data_type, config)
            elif strategy == 'by_date':
                SchedulerRuntime._build_by_date(data_type, config)
            elif strategy == 'by_period':
                SchedulerRuntime._build_by_period(data_type, config)
            elif strategy == 'by_exchange':
                SchedulerRuntime._build_by_exchange(data_type, config)
    
    @staticmethod
    def _build_by_stock(data_type: str, config: dict):
        """按股票批量构建"""
        logging.info(f"开始按股票构建{data_type}数据...")
        
        # 获取股票列表 - 这里可以使用字典模块获取股票列表
        try:
            from dictionaries import get_stock_list
            stock_codes = get_stock_list()
        except:
            # 如果无法获取股票列表，跳过构建
            logging.warning(f"无法获取股票列表，跳过{data_type}的按股票构建")
            return

        batch_size = config['update']['batch_size']
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i+batch_size]

            for ts_code in batch_codes:
                try:
                    df = download_data_by_config(data_type, ts_code=ts_code)
                    if df is not None and len(df) > 0:
                        EtlRuntime.process_data(data_type, df=df)
                except Exception as e:
                    logging.warning(f"下载{data_type}股票{ts_code}数据失败: {str(e)}")
                    continue

    @staticmethod
    def _build_by_date(data_type: str, config: dict):
        """按日期批量构建"""
        logging.info(f"开始按日期构建{data_type}数据...")
        
        # 从2005年开始构建（按要求从2005年1月1日开始）
        start_date = '20050101'
        end_date = datetime.now().strftime('%Y%m%d')

        # 使用交易日历获取交易日
        try:
            business_days = get_business_date_range(start_date, end_date)
        except:
            # 如果无法获取交易日历，使用所有日期
            current_date = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            business_days = []
            while current_date <= end_date_obj:
                business_days.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)

        for trade_date in business_days:
            try:
                df = download_data_by_config(data_type, trade_date=trade_date)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data(data_type, df=df)
            except Exception as e:
                logging.warning(f"下载{data_type}日期{trade_date}数据失败: {str(e)}")
                continue

    @staticmethod
    def _build_by_period(data_type: str, config: dict):
        """按报告期批量构建"""
        logging.info(f"开始按报告期构建{data_type}数据...")
        
        # 从2005年开始构建（按要求从2005年开始）
        start_year = 2005
        years = list(range(start_year, datetime.now().year + 1))
        quarters = ['0331', '0630', '0930', '1231']
        
        periods = []
        for year in years:
            for quarter in quarters:
                periods.append(f"{year}{quarter}")
                
                # 如果是当前年份，只构建到当前季度
                if year == datetime.now().year:
                    current_month = datetime.now().month
                    if current_month < 3:
                        break
                    elif current_month < 6:
                        if quarter == '0630':
                            break
                    elif current_month < 9:
                        if quarter == '0930':
                            break
                    elif current_month < 12:
                        if quarter == '1231':
                            break
                            
        batch_size = config['update']['batch_size']
        for i in range(0, len(periods), batch_size):
            batch_periods = periods[i:i+batch_size]
            
            for period in batch_periods:
                try:
                    df = download_data_by_config(data_type, period=period)
                    if df is not None and len(df) > 0:
                        EtlRuntime.process_data(data_type, df=df)
                except Exception as e:
                    logging.warning(f"下载{data_type}报告期{period}数据失败: {str(e)}")
                    continue

    @staticmethod
    def _build_by_exchange(data_type: str, config: dict):
        """按交易所构建"""
        logging.info(f"开始按交易所构建{data_type}数据...")
        
        exchanges = ['SSE', 'SZSE']  # 上海证券交易所和深圳证券交易所
        
        for exchange in exchanges:
            try:
                df = download_data_by_config(data_type, exchange=exchange)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data(data_type, df=df)
            except Exception as e:
                logging.warning(f"下载{data_type}交易所{exchange}数据失败: {str(e)}")
                continue

    @staticmethod
    def _find_first_missing_date(data_type: str) -> str:
        """
        智能检测：查找第一个缺失的日期
        这个函数会检查数据目录，找到第一个缺失的日期
        """
        config = DATA_INTERFACE_CONFIG[data_type]
        storage_path = config['storage']['path']
        
        # 根据存储路径和分区类型查找缺失的日期
        try:
            # 如果是分区存储（如YEAR或YEAR_MONTH）
            if config['storage']['partition_granularity'] != 'none':
                # 检查数据目录中的分区
                if storage_path.exists():
                    # 根据分区类型决定如何查找缺失日期
                    partition_type = config['storage']['partition_granularity']
                    
                    if partition_type == 'year':
                        # 按年分区，需要检查每年的数据
                        return SchedulerRuntime._find_missing_date_by_year(storage_path, data_type)
                    elif partition_type == 'year_month':
                        # 按年月分区，需要检查每月的数据
                        return SchedulerRuntime._find_missing_date_by_year_month(storage_path, data_type)
            else:
                # 如果是非分区存储，检查文件是否存在
                file_path = f"{storage_path}.parquet" if not str(storage_path).endswith('.parquet') else str(storage_path)
                if not Path(file_path).exists():
                    # 文件不存在，返回起始日期
                    return '20050101'

            # 如果当前日期的数据已存在，返回None表示没有缺失数据
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            return yesterday
            
        except Exception as e:
            logging.warning(f"检测{data_type}缺失日期时出错: {str(e)}")
            # 出错时返回昨天日期
            return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    @staticmethod
    def _find_missing_date_by_year(storage_path: Path, data_type: str) -> str:
        """按年分区查找缺失的日期"""
        # 获取从2005年到当前年份的所有年份
        current_year = datetime.now().year
        years = list(range(2005, current_year + 1))
        
        # 检查这些年份的分区是否存在
        for year in years:
            year_path = storage_path / f"year={year}"
            if not year_path.exists():
                # 如果年份分区不存在，返回该年1月1日
                return f"{year}0101"
            else:
                # 检查该年份内的数据是否完整
                data_file = year_path / "data.parquet"
                if not data_file.exists():
                    return f"{year}0101"
        
        # 所有年份都存在，返回昨天日期
        return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    @staticmethod
    def _find_missing_date_by_year_month(storage_path: Path, data_type: str) -> str:
        """按年月分区查找缺失的日期"""
        # 获取从2005年到当前年月的所有年月
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # 检查从2005年1月开始到当前年月的数据
        for year in range(2005, current_year + 1):
            start_month = 1 if year > 2005 else 1  # 从1月开始检查
            end_month = 12 if year < current_year else current_month  # 如果是当前年，只到当前月
            
            for month in range(start_month, end_month + 1):
                month_str = f"{year}{month:02d}"
                month_path = storage_path / f"year_month={month_str}"
                
                if not month_path.exists():
                    # 如果年月分区不存在，返回该月1日
                    return f"{month_str}01"
                else:
                    # 检查该月内的数据是否完整
                    data_file = month_path / "data.parquet"
                    if not data_file.exists():
                        return f"{month_str}01"
        
        # 所有年月都存在，返回昨天日期
        return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    @staticmethod
    def run_periodic_tasks():
        """运行周期性任务"""
        today = datetime.now()

        # 元数据更新：首先运行字典更新
        evolve_global_dictionaries()
        evolve_industry_dictionaries()
        evolve_area_dictionaries()
        update_stock_basic_with_ids()
        logging.info("元数据更新完成")

        # 每月初运行的任务
        if today.day == 1:
            # 更新券商荐股数据
            try:
                current_month = today.strftime('%Y%m')
                df = download_data_by_config('broker_recommend', month=current_month)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data('broker_recommend', df=df)
                    update_last_update_date('broker_recommend', current_month)
            except Exception as e:
                logging.warning(f"更新券商荐股数据失败: {str(e)}")

        # 每周一运行的任务
        if today.weekday() == 0:  # 0表示周一
            # 更新卖方盈利预测
            try:
                yesterday = (today - timedelta(days=1)).strftime('%Y%m%d')
                df = download_data_by_config('report_rc', ann_date=yesterday)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data('report_rc', df=df)
                    update_last_update_date('report_rc', yesterday)
            except Exception as e:
                logging.warning(f"更新卖方盈利预测数据失败: {str(e)}")

    @staticmethod
    def run_smart_initial_build(data_types=None, start_date='20050101', end_date=None):
        """运行智能初始构建（增强版），使用数据扫描和智能决策"""
        logging.info("开始智能初始构建（增强版）...")
        try:
            # 导入增强版构建函数
            from custom_build import build_with_enhanced_scan
            build_with_enhanced_scan(data_types, start_date, end_date)
            logging.info("智能初始构建（增强版）完成")
        except Exception as e:
            logging.error(f"智能初始构建（增强版）失败: {str(e)}")
            raise


class BatchStrategy:
    """批量构建策略"""
    
    @staticmethod
    def build_by_stock(data_type: str, config: dict):
        """按股票批量构建"""
        return SchedulerRuntime._build_by_stock(data_type, config)

    @staticmethod
    def build_by_date(data_type: str, config: dict):
        """按日期批量构建"""
        return SchedulerRuntime._build_by_date(data_type, config)

    @staticmethod
    def build_by_period(data_type: str, config: dict):
        """按报告期批量构建"""
        return SchedulerRuntime._build_by_period(data_type, config)