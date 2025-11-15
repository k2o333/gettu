# rebuild_partitioned_data.py
import logging
from datetime import datetime
from config import DATA_INTERFACE_CONFIG, PartitionGranularity
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime

def rebuild_partitioned_data(data_type=None, start_date='20050101', end_date=None):
    """
    重新下载数据以创建正确的分区结构
    这比迁移现有数据更可靠，避免了数据完整性问题
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    data_types_to_rebuild = [data_type] if data_type else [
        dt for dt, config in DATA_INTERFACE_CONFIG.items()
        if config['storage']['partition_granularity'] != PartitionGranularity.NONE
    ]

    for dt in data_types_to_rebuild:
        config = DATA_INTERFACE_CONFIG[dt]
        partition_granularity = config['storage']['partition_granularity']

        if partition_granularity == PartitionGranularity.NONE:
            continue  # 跳过非分区存储类型

        logging.info(f"重新下载 {dt} 数据以创建正确分区结构...")

        try:
            # 使用增强扫描模块的构建方法，它会使用修复后的ETL流程
            from enhanced_scanner import build_with_enhanced_scan
            build_with_enhanced_scan([dt], start_date, end_date)

            logging.info(f"{dt} 数据已重新下载并正确分区")

        except Exception as e:
            logging.error(f"重新下载 {dt} 数据失败: {str(e)}")
            # 不再提供备用降级方法，而是抛出异常
            raise RuntimeError(f"分区存储失败，不允许降级: {dt} - {str(e)}")

def _fallback_download_by_date_range(data_type, config, start_date, end_date):
    """备用下载方法，按日期范围逐步下载"""
    logging.info(f"使用备用方法下载 {data_type} 数据...")

    # 根据不同数据类型的批量策略执行构建
    strategy = config['update']['batch_strategy']
    if strategy == 'by_stock':
        _download_by_stock_with_date_range(data_type, config, start_date, end_date)
    elif strategy == 'by_date':
        _download_by_date_range(data_type, config, start_date, end_date)
    elif strategy == 'by_period':
        _download_by_period_range(data_type, config, start_date, end_date)
    else:
        # 默认策略
        try:
            df = download_data_by_config(data_type, start_date=start_date, end_date=end_date)
            if df is not None and len(df) > 0:
                EtlRuntime.process_data(data_type, df=df)
                logging.info(f"已处理{data_type}: {len(df)}条记录")
        except Exception as e:
            logging.error(f"下载{data_type}默认数据失败: {str(e)}")
            raise RuntimeError(f"分区存储失败，不允许降级: {str(e)}")

def _download_by_stock_with_date_range(data_type, config, start_date, end_date):
    """按股票重新下载（带日期范围）"""
    from dictionaries import get_stock_list

    logging.info(f"按股票重新下载{data_type}...")
    stock_codes = get_stock_list()
    batch_size = config['update']['batch_size']

    for i in range(0, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i+batch_size]

        for ts_code in batch_codes:
            try:
                df = download_data_by_config(data_type, ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data(data_type, df=df)
                    logging.debug(f"已处理{data_type}股票{ts_code}: {len(df)}条记录")
            except Exception as e:
                logging.warning(f"下载{data_type}股票{ts_code}数据失败: {str(e)}")
                continue

def _download_by_date_range(data_type, config, start_date, end_date):
    """按日期范围重新下载"""
    from dictionaries import get_business_date_range

    logging.info(f"按日期范围重新下载{data_type}...")
    try:
        business_days = get_business_date_range(start_date, end_date)
    except:
        from datetime import datetime as dt, timedelta
        current_date = dt.strptime(start_date, '%Y%m%d')
        end_date_obj = dt.strptime(end_date, '%Y%m%d')
        business_days = []
        while current_date <= end_date_obj:
            business_days.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)

    batch_size = config['update']['batch_size']
    for i in range(0, len(business_days), batch_size):
        batch_days = business_days[i:i+batch_size]

        for trade_date in batch_days:
            try:
                df = download_data_by_config(data_type, trade_date=trade_date)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data(data_type, df=df)
                    logging.debug(f"已处理{data_type}日期{trade_date}")
            except Exception as e:
                logging.warning(f"下载{data_type}日期{trade_date}数据失败: {str(e)}")
                continue

def _download_by_period_range(data_type, config, start_date, end_date):
    """按报告期范围重新下载"""
    logging.info(f"按报告期范围重新下载{data_type}...")

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = list(range(start_year, end_year + 1))
    quarters = ['0331', '0630', '0930', '1231']

    periods = []
    for year in years:
        for quarter in quarters:
            period = f"{year}{quarter}"
            if period >= start_date[:6] + '01' and period <= end_date[:6] + '31':
                periods.append(period)

                if year == end_year:
                    current_month = int(end_date[4:6])
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
                    logging.debug(f"已处理{data_type}报告期{period}")
            except Exception as e:
                logging.warning(f"下载{data_type}报告期{period}数据失败: {str(e)}")
                continue