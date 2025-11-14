import logging
from datetime import datetime
from config import DATA_INTERFACE_CONFIG
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime
from dictionaries import get_business_date_range

def build_with_date_range(data_types=None, start_date='20050101', end_date=None):
    """使用指定日期范围构建数据"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    
    types_to_build = data_types or DATA_INTERFACE_CONFIG.keys()

    for data_type in types_to_build:
        if data_type not in DATA_INTERFACE_CONFIG:
            logging.warning(f"未知数据类型: {data_type}")
            continue
        
        config = DATA_INTERFACE_CONFIG[data_type]
        logging.info(f"开始构建 {data_type} (日期范围: {start_date} - {end_date})...")

        try:
            # 根据不同的数据类型和批量策略执行构建
            strategy = config['update']['batch_strategy']
            if strategy == 'by_stock':
                _build_by_stock_with_date_range(data_type, config, start_date, end_date)
            elif strategy == 'by_date':
                _build_by_date_range(data_type, config, start_date, end_date)
            elif strategy == 'by_period':
                _build_by_period_range(data_type, config, start_date, end_date)
            elif strategy == 'by_exchange':
                _build_by_exchange_with_date_range(data_type, config, start_date, end_date)
            else:
                # 默认策略：尝试直接下载指定日期范围的数据
                _build_default_with_date_range(data_type, config, start_date, end_date)
                
        except Exception as e:
            logging.error(f"构建{data_type}失败: {str(e)}")
            continue


def _build_by_stock_with_date_range(data_type, config, start_date, end_date):
    """按股票构建（带日期范围）"""
    logging.info(f"按股票构建{data_type}...")
    
    # 获取股票列表
    try:
        from dictionaries import get_stock_list
        stock_codes = get_stock_list()
    except:
        logging.warning(f"无法获取股票列表，跳过{data_type}的按股票构建")
        return

    batch_size = config['update']['batch_size']
    for i in range(0, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i+batch_size]

        for ts_code in batch_codes:
            try:
                df = download_data_by_config(data_type, ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is not None and len(df) > 0:
                    EtlRuntime.process_data(data_type, df=df)
                    logging.info(f"已处理{data_type}股票{ts_code}: {len(df)}条记录")
            except Exception as e:
                logging.warning(f"下载{data_type}股票{ts_code}数据失败: {str(e)}")
                continue


def _build_by_date_range(data_type, config, start_date, end_date):
    """按日期范围构建"""
    logging.info(f"按日期范围构建{data_type}...")
    
    try:
        # 使用交易日历获取交易日
        business_days = get_business_date_range(start_date, end_date)
    except:
        # 如果无法获取交易日历，使用所有日期
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
                    logging.info(f"已处理{data_type}日期{trade_date}")
            except Exception as e:
                logging.warning(f"下载{data_type}日期{trade_date}数据失败: {str(e)}")
                continue


def _build_by_period_range(data_type, config, start_date, end_date):
    """按报告期范围构建"""
    logging.info(f"按报告期范围构建{data_type}...")
    
    # 计算年份范围
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
                
                # 如果是结束年份，只添加到当前季度
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
                    logging.info(f"已处理{data_type}报告期{period}")
            except Exception as e:
                logging.warning(f"下载{data_type}报告期{period}数据失败: {str(e)}")
                continue


def _build_by_exchange_with_date_range(data_type, config, start_date, end_date):
    """按交易所构建（带日期范围）"""
    logging.info(f"按交易所构建{data_type}...")
    
    exchanges = ['SSE', 'SZSE']  # 上海证券交易所和深圳证券交易所
    
    for exchange in exchanges:
        try:
            # 对于按交易所构建的数据，可能还需要日期参数
            df = download_data_by_config(data_type, exchange=exchange)
            if df is not None and len(df) > 0:
                EtlRuntime.process_data(data_type, df=df)
                logging.info(f"已处理{data_type}交易所{exchange}")
        except Exception as e:
            logging.warning(f"下载{data_type}交易所{exchange}数据失败: {str(e)}")
            continue


def _build_default_with_date_range(data_type, config, start_date, end_date):
    """默认构建方法（带日期范围）"""
    logging.info(f"使用默认方法构建{data_type}...")
    
    try:
        # 尝试使用日期范围参数下载数据
        df = download_data_by_config(data_type, start_date=start_date, end_date=end_date)
        if df is not None and len(df) > 0:
            EtlRuntime.process_data(data_type, df=df)
            logging.info(f"已处理{data_type}: {len(df)}条记录")
    except Exception as e:
        logging.warning(f"使用默认方法下载{data_type}数据失败: {str(e)}")
        
        # 如果带日期范围的下载失败，尝试不带日期的下载
        try:
            df = download_data_by_config(data_type)
            if df is not None and len(df) > 0:
                EtlRuntime.process_data(data_type, df=df)
                logging.info(f"已处理{data_type}(无日期范围): {len(df)}条记录")
        except Exception as e2:
            logging.error(f"下载{data_type}数据彻底失败: {str(e2)}")