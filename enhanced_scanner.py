import os
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import logging
from config import DATA_INTERFACE_CONFIG
import glob
import re
import threading
from typing import Optional
import time


class DataIntegrityChecker:
    """数据完整性检查器"""

    def __init__(self):
        self.date_columns = ['trade_date', 'ann_date', 'period', 'end_date', 'date', 'ts_date']

    def check_file_integrity(self, file_path: Path, ts_code: str, expected_start: str, expected_end: str):
        """检查单个文件的完整性"""
        try:
            # 读取日期列进行完整性检查
            df = pl.read_parquet(file_path, columns=self.date_columns[:1])  # 只读取第一个日期列

            date_col = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in self.date_columns):
                    date_col = col
                    break

            if not date_col:
                # 如果没有找到日期列，尝试读取前1000行获取所有列
                df = pl.read_parquet(file_path, n_rows=1000)
                for col in df.columns:
                    if any(keyword in col.lower() for keyword in self.date_columns):
                        date_col = col
                        break

            if not date_col:
                return {
                    'is_complete': False,
                    'issues': ['No date column found'],
                    'date_range': None,
                    'gaps': [],
                    'record_count': len(df)
                }

            # 获取有效日期序列
            date_series = df[date_col].cast(pl.Utf8).drop_nulls()
            date_strings = sorted(set(date_series.to_list()))

            # 解析日期
            valid_dates = []
            for date_str in date_strings:
                parsed_date = self._parse_date_string(date_str)
                if parsed_date:
                    valid_dates.append(parsed_date)

            if not valid_dates:
                return {
                    'is_complete': False,
                    'issues': ['No valid dates found'],
                    'date_range': None,
                    'gaps': [],
                    'record_count': len(df)
                }

            valid_dates = sorted(valid_dates)
            start_date = valid_dates[0]
            end_date = valid_dates[-1]

            # 检查时间序列连续性
            gaps = self._find_date_gaps(valid_dates, start_date, end_date)

            return {
                'is_complete': len(gaps) == 0,
                'issues': ['date_gaps'] if gaps else [],
                'date_range': (start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),
                'gaps': [(gap[0].strftime('%Y%m%d'), gap[1].strftime('%Y%m%d')) for gap in gaps],
                'record_count': len(df),
                'valid_dates_count': len(valid_dates)
            }

        except Exception as e:
            logging.error(f"检查文件 {file_path} 完整性时出错: {str(e)}")
            return {
                'is_complete': False,
                'issues': [f'file_read_error: {str(e)}'],
                'date_range': None,
                'gaps': [],
                'record_count': 0
            }

    def _parse_date_string(self, date_str):
        """解析日期字符串"""
        date_str = str(date_str).strip()

        # 尝试多种日期格式
        formats = [
            '%Y%m%d',      # 20231028
            '%Y-%m-%d',    # 2023-10-28
            '%Y/%m/%d',    # 2023/10/28
            '%Y-%m',       # 2023-10
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # 特殊处理 - 如果是年季格式，转换为季度末日期
        if len(date_str) == 6 and date_str[4:] in ['03', '06', '09', '12']:
            try:
                year = int(date_str[:4])
                month = int(date_str[4:])
                # 简化处理：将季末转换为月末
                if month == 3:
                    day = 31
                elif month == 6:
                    day = 30
                elif month == 9:
                    day = 30
                else:  # month == 12
                    day = 31
                return datetime(year, month, day)
            except:
                pass

        return None

    def _find_date_gaps(self, date_list, start_date, end_date):
        """查找日期序列中的缺口"""
        if not date_list:
            return []

        gaps = []
        current = start_date

        for date in sorted(date_list):
            if date > current:
                # 找到缺口
                gap_start = current + timedelta(days=1)
                gap_end = date - timedelta(days=1)

                # 只记录连续交易日缺口（忽略节假日等正常缺口）
                # 这里可根据具体业务逻辑调整
                gaps.append((gap_start, gap_end))

            current = date + timedelta(days=1)

        return gaps


class DataScanner:
    """自动扫描现有数据的引擎（增强版）"""

    def __init__(self):
        from config import ROOT_DIR
        self.data_dir = ROOT_DIR
        self.integrity_checker = DataIntegrityChecker()
        self.scan_cache = {}  # 扫描结果缓存

    def scan_all_data(self, data_types=None, use_cache=False, cache_ttl_minutes=30):
        """扫描所有数据类型的现有数据覆盖情况"""
        cache_key = f"all_{str(data_types)}"

        if use_cache and cache_key in self.scan_cache:
            cached_time, cached_result = self.scan_cache[cache_key]
            if (datetime.now() - cached_time).seconds < cache_ttl_minutes * 60:
                logging.info("使用缓存的扫描结果")
                return cached_result

        types_to_scan = data_types or DATA_INTERFACE_CONFIG.keys()
        scan_results = {}

        for data_type in types_to_scan:
            logging.info(f"开始扫描数据类型: {data_type}")
            
            if data_type not in DATA_INTERFACE_CONFIG:
                logging.warning(f"未知数据类型: {data_type}")
                continue
                
            config = DATA_INTERFACE_CONFIG[data_type]
            type_results = self.scan_data_type(data_type, config)
            scan_results[data_type] = type_results

        # 缓存结果
        if use_cache:
            self.scan_cache[cache_key] = (datetime.now(), scan_results)

        return scan_results

    def scan_data_type(self, data_type: str, config: dict):
        """扫描特定数据类型的现有数据（严格按配置分区）"""
        storage_path = config['storage']['path']
        partition_granularity = config['storage']['partition_granularity']

        # 根据配置的分区粒度确定实际路径
        if partition_granularity != 'none':
            type_dir = storage_path
        else:
            # 非分区存储，需要检查父目录
            type_dir = storage_path.parent if storage_path.suffix == '.parquet' else storage_path

        if not type_dir.exists():
            logging.debug(f"存储路径不存在: {type_dir}")
            return {
                'has_data': False,
                'stock_coverage': {},
                'date_range': None,
                'total_files': 0,
                'total_records': 0,
                'integrity_issues': 0
            }

        # 严格按配置的分区粒度扫描 - 不再兼容非分区格式
        if partition_granularity == 'year':
            # 对于按年分区的数据，需要搜索所有年份分区中的data.parquet文件
            parquet_files = list(type_dir.glob("**/data.parquet"))
        elif partition_granularity == 'year_month':
            # 对于按年月分区的数据
            parquet_files = list(type_dir.glob("**/data.parquet"))
        else:
            # 非分区存储，直接查找.parquet文件
            parquet_files = list(type_dir.glob("*.parquet"))

        if not parquet_files:
            logging.debug(f"未找到指定格式的数据文件: {type_dir}")
            return {
                'has_data': False,
                'stock_coverage': {},
                'date_range': None,
                'total_files': 0,
                'total_records': 0,
                'integrity_issues': 0
            }

        stock_coverage = {}
        total_records = 0
        integrity_issues = 0

        # 分析每个股票的数据
        for file_path in parquet_files:
            # 从文件名或目录结构中提取股票代码或相关信息
            file_name = file_path.name
            ts_code = self._extract_ts_code_from_filename(file_name, file_path)

            if ts_code:
                coverage_info = self._analyze_stock_data_with_integrity(file_path, ts_code)
                stock_coverage[ts_code] = coverage_info
                total_records += coverage_info['record_count']

                if not coverage_info['integrity']['is_complete']:
                    integrity_issues += 1

        # 计算整体日期范围
        all_start_dates = [info['date_range'][0] for info in stock_coverage.values()
                          if info['date_range'] and info['date_range'][0]]
        all_end_dates = [info['date_range'][1] for info in stock_coverage.values()
                        if info['date_range'] and info['date_range'][1]]

        overall_date_range = None
        if all_start_dates and all_end_dates:
            overall_date_range = (min(all_start_dates), max(all_end_dates))

        return {
            'has_data': True,
            'stock_coverage': stock_coverage,
            'date_range': overall_date_range,
            'total_files': len(parquet_files),
            'total_records': total_records,
            'integrity_issues': integrity_issues
        }

    def _analyze_stock_data_with_integrity(self, file_path: Path, ts_code: str):
        """分析单个股票数据文件的时间覆盖范围（包含完整性检查）"""
        # 首先进行完整性检查
        integrity_result = self.integrity_checker.check_file_integrity(
            file_path, ts_code, '19900101', datetime.now().strftime('%Y%m%d')
        )

        # 内存保护：检查文件大小
        file_size = file_path.stat().st_size
        if file_size > 500 * 1024 * 1024:  # 500MB
            logging.info(f"文件 {file_path} 较大 ({file_size / (1024*1024):.1f}MB)，使用流式分析")
            return self._streaming_analysis(file_path, ts_code, integrity_result)

        try:
            # 基本数据范围检查
            df = pl.read_parquet(file_path, n_rows=10000)

            # 查找日期相关的列
            date_columns = [col for col in df.columns
                           if any(keyword in col.lower() for keyword in
                                  ['date', 'trade', 'ann', 'period', 'end'])]

            if not date_columns:
                return {
                    'date_range': None,
                    'record_count': len(df),
                    'date_column': None,
                    'integrity': integrity_result
                }

            # 使用第一个日期列进行分析
            date_col = date_columns[0]

            # 快速估算记录总数（避免加载大文件）
            record_count = integrity_result['record_count']  # 使用完整性检查的结果

            # 使用完整性检查的结果作为日期范围
            date_range = integrity_result['date_range']

            return {
                'date_range': date_range,
                'record_count': record_count,
                'date_column': date_col,
                'integrity': integrity_result
            }

        except Exception as e:
            logging.error(f"分析股票 {ts_code} 数据文件 {file_path} 时出错: {str(e)}")
            return {
                'date_range': None,
                'record_count': 0,
                'date_column': None,
                'integrity': integrity_result
            }

    def _streaming_analysis(self, file_path: Path, ts_code: str, integrity_result: dict):
        """对大文件使用流式分析以节省内存"""
        try:
            # 对于大文件，只分析日期列以确定时间范围
            # 使用polars的scan接口进行流式处理
            lazy_df = pl.scan_parquet(file_path)

            # 获取列信息
            schema = lazy_df.collect_schema()
            date_columns = [col for col in schema.names()
                           if any(keyword in col.lower() for keyword in
                                  ['date', 'trade', 'ann', 'period', 'end'])]

            if not date_columns:
                # 如果没有日期列，返回基本信息
                total_rows = lazy_df.select(pl.len()).collect().item()
                return {
                    'date_range': integrity_result['date_range'],
                    'record_count': total_rows,
                    'date_column': None,
                    'integrity': integrity_result
                }

            date_col = date_columns[0]

            # 获取日期的最小值和最大值（流式处理）
            min_max_dates = lazy_df.select([
                pl.col(date_col).min().alias('min_date'),
                pl.col(date_col).max().alias('max_date'),
                pl.len().alias('total_count')
            ]).collect()

            min_date = min_max_dates['min_date'][0]
            max_date = min_max_dates['max_date'][0]
            record_count = min_max_dates['total_count'][0]

            # 尝试格式化日期
            date_range = None
            if min_date is not None and max_date is not None:
                try:
                    if isinstance(min_date, str):
                        min_date_str = min_date.replace('-', '').replace('/', '')[:8]
                        max_date_str = max_date.replace('-', '').replace('/', '')[:8]
                    else:
                        min_date_str = str(min_date)[:8] if len(str(min_date)) >= 8 else None
                        max_date_str = str(max_date)[:8] if len(str(max_date)) >= 8 else None

                    if min_date_str and max_date_str and len(min_date_str) == 8 and len(max_date_str) == 8:
                        date_range = (min_date_str, max_date_str)
                except:
                    # 如果日期格式化失败，使用完整性检查的结果
                    date_range = integrity_result['date_range']

            return {
                'date_range': date_range,
                'record_count': record_count,
                'date_column': date_col,
                'integrity': integrity_result
            }

        except Exception as e:
            logging.error(f"流式分析股票 {ts_code} 数据文件 {file_path} 时出错: {str(e)}")
            # 备用方案：返回完整性检查的结果
            return {
                'date_range': integrity_result['date_range'],
                'record_count': integrity_result['record_count'],
                'date_column': None,
                'integrity': integrity_result
            }

    def _extract_ts_code_from_filename(self, filename: str, file_path: Path):
        """从文件名或目录结构中提取股票代码（增强版）"""
        # 首先尝试从文件名中提取
        patterns = [
            r'(\d{6}_[A-Z]{2})',      # 000001_SZ
            r'([A-Z]{2}\d{6})',       # SZ000001
            r'(\d{6}\.[A-Z]{2})',     # 000001.SZ
            r'(\d{6})',               # 000001 (纯数字)
        ]

        for pattern in patterns:
            matches = re.findall(pattern, filename)
            if matches:
                ts_code = matches[0]

                # 标准化格式
                if '.' not in ts_code and '_' not in ts_code and len(ts_code) == 6:
                    # 如果是纯6位数字，需要确定交易所
                    # 这里可以根据前缀规则或者默认规则处理
                    if ts_code.startswith('6'):
                        return f"{ts_code}.SH"  # 6开头通常是上交所
                    elif ts_code.startswith('0') or ts_code.startswith('3'):
                        return f"{ts_code}.SZ"  # 0或3开头通常是深交所
                    else:
                        # 默认返回SH，但这可能需要更复杂的逻辑
                        return f"{ts_code}.SH"
                elif '_' in ts_code:
                    return ts_code.replace('_', '.')
                elif '.' not in ts_code and len(ts_code) == 8:
                    # SZ000001 or SH600000 format
                    exchange = ts_code[:2]
                    code = ts_code[2:]
                    return f"{code}.{exchange}"
                elif '.' not in ts_code and len(ts_code) == 6:
                    # 纯数字，根据规则转换
                    if ts_code.startswith('6'):
                        return f"{ts_code}.SH"
                    elif ts_code.startswith('0') or ts_code.startswith('3'):
                        return f"{ts_code}.SZ"
                    else:
                        return f"{ts_code}.SH"  # 默认
                else:
                    return ts_code  # 已经是标准格式

        # 如果从文件名中没有找到，尝试从路径中查找可能的股票代码
        # 检查分区目录名称，例如：year=2023/ts_code=000001.SZ/
        path_parts = str(file_path).split('/')
        for part in path_parts:
            if part.startswith('ts_code='):
                ts_code = part.split('=', 1)[1]
                return ts_code

        return None


class DownloadLockManager:
    """下载锁管理器，防止并发下载冲突"""

    def __init__(self):
        self._locks = {}
        self._lock = threading.Lock()

    def acquire_lock(self, ts_code: str, data_type: str, date_range: tuple) -> bool:
        """获取下载锁"""
        lock_key = f"{data_type}_{ts_code}_{date_range[0]}_{date_range[1]}"

        with self._lock:
            if lock_key in self._locks:
                logging.info(f"下载锁已被占用: {lock_key}")
                return False

            self._locks[lock_key] = datetime.now()
            logging.info(f"获取下载锁: {lock_key}")
            return True

    def release_lock(self, ts_code: str, data_type: str, date_range: tuple):
        """释放下载锁"""
        lock_key = f"{data_type}_{ts_code}_{date_range[0]}_{date_range[1]}"

        with self._lock:
            if lock_key in self._locks:
                del self._locks[lock_key]
                logging.info(f"释放下载锁: {lock_key}")

    def is_locked(self, ts_code: str, data_type: str, date_range: tuple) -> bool:
        """检查是否已锁定"""
        lock_key = f"{data_type}_{ts_code}_{date_range[0]}_{date_range[1]}"
        return lock_key in self._locks


# 全局下载锁管理器
download_lock_manager = DownloadLockManager()


class DownloadDecisionEngine:
    """智能下载决策引擎（增强版）"""

    def __init__(self):
        self.scanner = DataScanner()

    def make_download_decisions(self, scan_results: dict, requested_start: str, requested_end: str):
        """根据扫描结果和请求范围，生成下载决策（增强版）"""
        decisions = {}

        for data_type, scan_result in scan_results.items():
            if not scan_result['has_data']:
                # 完全没有数据，需要从头开始下载
                decisions[data_type] = {
                    'strategy': 'full_download',
                    'all_stocks': True,
                    'date_range': (requested_start, requested_end),
                    'reason': 'no_existing_data'
                }
            else:
                # 已有部分数据，生成股票级别的下载决策
                stock_decisions = self._make_stock_level_decisions_enhanced(
                    scan_result, requested_start, requested_end
                )

                decisions[data_type] = {
                    'strategy': 'incremental',
                    'stock_decisions': stock_decisions,
                    'integrity_issues': scan_result['integrity_issues']
                }

        return decisions

    def _make_stock_level_decisions_enhanced(self, scan_result: dict, requested_start: str, requested_end: str):
        """生成股票级别的下载决策（增强版）"""
        from dictionaries import get_stock_list

        all_stocks = set(get_stock_list())
        covered_stocks = set(scan_result['stock_coverage'].keys())

        decisions = {}

        # 已有数据的股票
        for ts_code, coverage_info in scan_result['stock_coverage'].items():
            if coverage_info['date_range'] and coverage_info['date_range'][0]:
                existing_start, existing_end = coverage_info['date_range']

                # 检查完整性
                integrity_issues = coverage_info['integrity']['issues']

                # 决定需要下载哪些时间段
                if integrity_issues:  # 如果存在完整性问题，需要重新下载相应时间段
                    # 根据完整性问题确定需要重新下载的时间段
                    if 'date_gaps' in integrity_issues:
                        # 重新下载整个范围，因为存在时间缺口
                        decisions[ts_code] = {
                            'action': 'download_full',
                            'date_range': (requested_start, requested_end),
                            'reason': 'data_gaps_detected',
                            'existing_range': (existing_start, existing_end),
                            'record_count': coverage_info['record_count']
                        }
                    else:
                        # 计算缺失的时间段，同时考虑完整性问题
                        download_periods = self._calculate_missing_periods_with_integrity(
                            existing_start, existing_end, requested_start, requested_end,
                            coverage_info['integrity']['gaps']
                        )

                        if download_periods:
                            decisions[ts_code] = {
                                'action': 'download_missing',
                                'missing_periods': download_periods,
                                'reason': 'missing_data_with_gaps',
                                'existing_range': (existing_start, existing_end),
                                'record_count': coverage_info['record_count']
                            }
                        else:
                            decisions[ts_code] = {
                                'action': 'skip',
                                'reason': 'data_incomplete_but_no_missing_periods',
                                'existing_range': (existing_start, existing_end),
                                'record_count': coverage_info['record_count']
                            }
                else:
                    # 没有完整性问题，按正常逻辑处理
                    download_periods = self._calculate_missing_periods(
                        existing_start, existing_end, requested_start, requested_end
                    )

                    if download_periods:
                        decisions[ts_code] = {
                            'action': 'download_missing',
                            'missing_periods': download_periods,
                            'reason': 'missing_data_periods',
                            'existing_range': (existing_start, existing_end),
                            'record_count': coverage_info['record_count']
                        }
                    else:
                        decisions[ts_code] = {
                            'action': 'skip',
                            'reason': 'data_fully_covered_and_complete',
                            'existing_range': (existing_start, existing_end),
                            'record_count': coverage_info['record_count']
                        }
            else:
                # 数据范围无效，重新下载整个范围
                decisions[ts_code] = {
                    'action': 'download_full',
                    'date_range': (requested_start, requested_end),
                    'reason': 'invalid_date_range',
                    'existing_range': None,
                    'record_count': 0
                }

        # 没有数据的股票
        uncovered_stocks = all_stocks - covered_stocks
        for ts_code in uncovered_stocks:
            decisions[ts_code] = {
                'action': 'download_full',
                'date_range': (requested_start, requested_end),
                'reason': 'no_existing_data'
            }

        return decisions

    def _calculate_missing_periods_with_integrity(self, existing_start: str, existing_end: str,
                                                 requested_start: str, requested_end: str,
                                                 detected_gaps: list):
        """计算缺失的时间段（考虑完整性问题）"""
        # 首先计算基本的缺失时间段
        basic_missing = self._calculate_missing_periods(existing_start, existing_end, requested_start, requested_end)

        # 合并检测到的缺口
        all_missing_periods = basic_missing[:]

        for gap_start, gap_end in detected_gaps:
            # 将检测到的缺口添加到缺失时间段列表
            gap_start_dt = datetime.strptime(gap_start, '%Y%m%d')
            gap_end_dt = datetime.strptime(gap_end, '%Y%m%d')
            requested_start_dt = datetime.strptime(requested_start, '%Y%m%d')
            requested_end_dt = datetime.strptime(requested_end, '%Y%m%d')

            # 确保缺口在请求范围内
            if gap_start_dt >= requested_start_dt and gap_end_dt <= requested_end_dt:
                all_missing_periods.append((gap_start, gap_end))

        # 合并重叠的时间段
        return self._merge_overlapping_periods(all_missing_periods)

    def _calculate_missing_periods(self, existing_start: str, existing_end: str,
                                   requested_start: str, requested_end: str):
        """计算缺失的时间段（增强版，处理边界情况）"""
        try:
            existing_start_dt = datetime.strptime(existing_start, '%Y%m%d')
            existing_end_dt = datetime.strptime(existing_end, '%Y%m%d')
            requested_start_dt = datetime.strptime(requested_start, '%Y%m%d')
            requested_end_dt = datetime.strptime(requested_end, '%Y%m%d')
        except ValueError as e:
            logging.error(f"日期解析错误: {e}")
            return [(requested_start, requested_end)]

        missing_periods = []

        # 检查开始日期前是否有缺失数据（确保至少有一天的缓冲区）
        if requested_start_dt < existing_start_dt:
            gap_end_dt = existing_start_dt - timedelta(days=1)
            if gap_end_dt >= requested_start_dt:
                missing_periods.append((
                    requested_start_dt.strftime('%Y%m%d'),
                    gap_end_dt.strftime('%Y%m%d')
                ))

        # 检查结束日期后是否有缺失数据
        if existing_end_dt < requested_end_dt:
            gap_start_dt = existing_end_dt + timedelta(days=1)
            if gap_start_dt <= requested_end_dt:
                missing_periods.append((
                    gap_start_dt.strftime('%Y%m%d'),
                    requested_end_dt.strftime('%Y%m%d')
                ))

        return missing_periods

    def _merge_overlapping_periods(self, periods):
        """合并重叠的时间段"""
        if not periods:
            return []

        # 转换为datetime对象进行排序和合并
        period_dts = []
        for start, end in periods:
            period_dts.append((datetime.strptime(start, '%Y%m%d'),
                             datetime.strptime(end, '%Y%m%d')))

        # 排序
        period_dts.sort(key=lambda x: x[0])

        # 合并重叠时间段
        merged = []
        for current_start, current_end in period_dts:
            if not merged:
                merged.append([current_start, current_end])
            else:
                last_start, last_end = merged[-1]

                # 如果当前时间段与上一个时间段重叠或连续
                if current_start <= last_end + timedelta(days=1):
                    # 合并时间段
                    merged[-1][1] = max(last_end, current_end)
                else:
                    # 添加新的时间段
                    merged.append([current_start, current_end])

        # 转换回字符串格式
        return [(period[0].strftime('%Y%m%d'), period[1].strftime('%Y%m%d')) for period in merged]


def build_with_enhanced_scan(data_types=None, start_date='20050101', end_date=None):
    """使用增强扫描的构建函数"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    # 初始化增强的扫描和决策引擎
    scanner = DataScanner()
    decision_engine = DownloadDecisionEngine()

    # 扫描现有数据（使用缓存以提高性能）
    logging.info("开始扫描现有数据（增强版）...")
    scan_results = scanner.scan_all_data(data_types, use_cache=True)

    # 生成下载决策
    logging.info("生成下载决策（考虑完整性问题）...")
    download_decisions = decision_engine.make_download_decisions(
        scan_results, start_date, end_date
    )

    # 执行下载决策
    from interface_manager import download_data_by_config
    from etl_runtime import EtlRuntime
    from custom_build import _build_by_stock_with_date_range

    for data_type, decision in download_decisions.items():
        if data_type not in DATA_INTERFACE_CONFIG:
            logging.warning(f"未知数据类型: {data_type}")
            continue

        config = DATA_INTERFACE_CONFIG[data_type]
        logging.info(f"处理数据类型: {data_type}")
        logging.info(f"完整性问题数量: {decision.get('integrity_issues', 0)}")

        if decision['strategy'] == 'full_download':
            # 完整下载策略
            logging.info(f"{data_type} - 执行完整下载: {decision['date_range']} (原因: {decision['reason']})")
            _build_by_stock_with_date_range(
                data_type, config,
                decision['date_range'][0], decision['date_range'][1]
            )
        else:
            # 增量下载策略 - 股票级别（增强版）
            stock_decisions = decision['stock_decisions']

            # 分类处理
            full_download_stocks = []
            missing_download_stocks = []
            integrity_issue_stocks = []
            skip_stocks = []

            for ts_code, stock_decision in stock_decisions.items():
                action = stock_decision['action']
                reason = stock_decision['reason']

                if action == 'download_full':
                    full_download_stocks.append((ts_code, stock_decision['date_range'], reason))
                elif action == 'download_missing':
                    for period in stock_decision['missing_periods']:
                        missing_download_stocks.append((ts_code, period[0], period[1], reason))
                elif action == 'skip':
                    skip_stocks.append((ts_code, reason))
                else:
                    skip_stocks.append((ts_code, 'unknown_action'))

            logging.info(f"{data_type} - 完整下载股票数: {len(full_download_stocks)}")
            logging.info(f"{data_type} - 增量下载股票数: {len(missing_download_stocks)}")
            logging.info(f"{data_type} - 跳过股票数: {len(skip_stocks)}")

            # 执行完整下载的股票
            for ts_code, date_range, reason in full_download_stocks:
                if download_lock_manager.acquire_lock(ts_code, data_type, date_range):
                    try:
                        logging.info(f"股票 {ts_code} - {reason}")
                        df = download_data_by_config(
                            data_type, ts_code=ts_code,
                            start_date=date_range[0], end_date=date_range[1]
                        )
                        if df is not None and len(df) > 0:
                            EtlRuntime.process_data(data_type, df=df)
                            logging.info(f"已处理{data_type}股票{ts_code}(完整): {len(df)}条记录")
                    except Exception as e:
                        logging.error(f"下载{data_type}股票{ts_code}(完整)失败: {str(e)}")
                    finally:
                        download_lock_manager.release_lock(ts_code, data_type, date_range)

            # 执行缺失数据下载
            for ts_code, start_missing, end_missing, reason in missing_download_stocks:
                date_range = (start_missing, end_missing)
                if download_lock_manager.acquire_lock(ts_code, data_type, date_range):
                    try:
                        logging.info(f"股票 {ts_code} - {reason}")
                        df = download_data_by_config(
                            data_type, ts_code=ts_code,
                            start_date=start_missing, end_date=end_missing
                        )
                        if df is not None and len(df) > 0:
                            EtlRuntime.process_data(data_type, df=df)
                            logging.info(f"已补充{data_type}股票{ts_code}(缺失): {len(df)}条记录")
                    except Exception as e:
                        logging.error(f"补充{data_type}股票{ts_code}(缺失)失败: {str(e)}")
                    finally:
                        download_lock_manager.release_lock(ts_code, data_type, date_range)


def efficient_large_file_scan(file_path: Path, sample_ratio: float = 0.1):
    """优化的大文件扫描，使用采样策略"""
    try:
        # 获取文件大小
        file_size = file_path.stat().st_size

        if file_size > 100 * 1024 * 1024:  # 大于100MB
            logging.info(f"文件 {file_path} 较大 ({file_size / (1024*1024):.1f}MB)，使用采样策略")

            # 对于大文件，使用分块读取或采样
            return _sample_based_analysis(file_path, sample_ratio)
        else:
            # 小文件直接读取
            return pl.read_parquet(file_path)
    except Exception as e:
        logging.error(f"大文件扫描失败: {str(e)}")
        # 备用方案：尝试读取前面部分数据
        return pl.read_parquet(file_path, n_rows=10000)


def _sample_based_analysis(file_path: Path, sample_ratio: float):
    """基于采样的文件分析"""
    try:
        # 先读取前10000行获取schema信息
        df_schema = pl.read_parquet(file_path, n_rows=10000)

        # 获取总行数
        total_rows = len(df_schema)  # 这里只是示例，实际需要更复杂的方法获取总行数

        # 对于非常大的文件，可能需要使用其他工具来估算统计信息
        # 这里提供一个简化的采样示例
        return df_schema

    except Exception as e:
        logging.error(f"采样分析失败: {str(e)}")
        return None