from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from config import DATA_INTERFACE_CONFIG
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime


class RateLimitManager:
    """速率限制管理器"""
    def __init__(self):
        self.limit_stats = defaultdict(list)  # 存储每个接口的调用时间
        self.lock = threading.Lock()

    def can_make_request(self, api_name: str, max_requests: int, time_window: int = 60):
        """检查是否可以发起API请求"""
        with self.lock:
            now = datetime.now()
            # 清理过期的请求记录
            self.limit_stats[api_name] = [
                req_time for req_time in self.limit_stats[api_name]
                if (now - req_time).seconds < time_window
            ]

            # 检查是否超过限制
            if len(self.limit_stats[api_name]) >= max_requests:
                return False

            # 记录当前请求
            self.limit_stats[api_name].append(now)
            return True

    def get_wait_time(self, api_name: str, max_requests: int, time_window: int = 60):
        """获取需要等待的时间"""
        with self.lock:
            if len(self.limit_stats[api_name]) == 0:
                return 0

            # 按时间排序
            times = sorted(self.limit_stats[api_name])
            oldest_time = times[0]
            elapsed = (datetime.now() - oldest_time).seconds
            return max(0, time_window - elapsed)


class DailyLimitManager:
    """管理每日请求限制的接口（如report_rc每天10次限制）"""
    def __init__(self):
        self.daily_stats = defaultdict(list)  # 存储每个接口的每日调用记录
        self.lock = threading.Lock()

    def can_make_request_today(self, api_name: str, max_daily_requests: int):
        """检查今天是否还能发起API请求"""
        with self.lock:
            today = datetime.now().date()
            # 清理过期的请求记录
            self.daily_stats[api_name] = [
                req_datetime for req_datetime in self.daily_stats[api_name]
                if req_datetime.date() == today
            ]

            # 检查今日是否超过限制
            if len(self.daily_stats[api_name]) >= max_daily_requests:
                return False

            # 记录当前请求
            self.daily_stats[api_name].append(datetime.now())
            return True

    def get_remaining_daily_requests(self, api_name: str, max_daily_requests: int):
        """获取今天的剩余请求次数"""
        with self.lock:
            today = datetime.now().date()
            # 清理过期的请求记录
            self.daily_stats[api_name] = [
                req_datetime for req_datetime in self.daily_stats[api_name]
                if req_datetime.date() == today
            ]

            return max(0, max_daily_requests - len(self.daily_stats[api_name]))


class OptimizedDataDownloader:
    def __init__(self, max_workers=10):
        self.rate_limiter = RateLimitManager()
        self.daily_limiter = DailyLimitManager()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.max_workers = max_workers

    def download_with_retry(self, data_type: str, params: Dict[str, Any], max_retries: int = 3) -> Any:
        """带重试机制的下载"""
        for attempt in range(max_retries):
            try:
                result = download_data_by_config(data_type, **params)
                return result
            except Exception as e:
                error_msg = str(e)
                if "权限" in error_msg or "速率" in error_msg or "限制" in error_msg:
                    # 遇到速率限制，等待后重试
                    wait_time = 60 * (attempt + 1)  # 递增等待时间
                    logging.warning(f"{data_type} 遇到速率限制，等待 {wait_time} 秒后重试 (尝试 {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise e
        raise Exception(f"多次重试后下载 {data_type} 仍失败")

    def download_single_data_type(self, data_type: str):
        """下载单个数据类型的数据（用于测试）"""
        config = DATA_INTERFACE_CONFIG[data_type]
        supports = config['supports']

        # 检查是否有每日限制
        daily_limit = config.get('daily_limit', None)

        # 对于有每日限制的接口，在测试时也进行处理，但采用更灵活的策略
        if daily_limit is not None:
            # 检查是否还有当日请求次数
            if not self.daily_limiter.can_make_request_today(data_type, daily_limit):
                logging.info(f"测试: {data_type} 已达到每日请求限制，跳过测试")
                return None
            else:
                # 对于有每日限制的接口，可能需要分页下载以充分利用每日限制
                # 但在测试场景中，我们先尝试正常下载
                result = self._download_single_data_type_with_pagination(data_type)
                return result
        else:
            # 普通接口按正常流程下载
            # 构建适当的参数（针对测试场景使用2019年的数据）
            kwargs = self.build_test_parameters(data_type, supports)

            # 检查速率限制
            api_limit = config.get('api_limit', 500)  # 默认限制
            api_name = config.get('api_name', data_type)

            while not self.rate_limiter.can_make_request(api_name, api_limit // 10):
                wait_time = self.rate_limiter.get_wait_time(api_name, api_limit // 10)
                logging.info(f"等待 {wait_time} 秒以避免 {api_name} 接口速率限制")
                time.sleep(wait_time)

            # 下载数据
            for attempt in range(3):  # 最多重试3次
                try:
                    df = self.download_with_retry(data_type, kwargs)
                    return df
                except Exception as e:
                    error_msg = str(e)
                    if "权限" in error_msg or "速率" in error_msg or "限制" in error_msg:
                        wait_time = 60 * (attempt + 1)
                        logging.warning(f"{data_type} 遇到速率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"下载 {data_type} 时出错: {error_msg}")
                        break

        return None

    def _download_single_data_type_with_pagination(self, data_type: str):
        """为有每日限制的接口进行分页下载，用于测试"""
        config = DATA_INTERFACE_CONFIG[data_type]
        supports = config['supports']

        # 构建适当的参数（针对测试场景使用2019年的数据）
        kwargs = self.build_test_parameters(data_type, supports)

        # 检查每日请求限制
        daily_limit = config.get('daily_limit', 10)  # 默认每日10次
        remaining_requests = self.daily_limiter.get_remaining_daily_requests(data_type, daily_limit)
        if remaining_requests <= 0:
            logging.info(f"测试: {data_type} 已达到每日请求限制")
            return None

        # 检查速率限制
        api_limit = config.get('api_limit', 500)
        api_name = config.get('api_name', data_type)

        # 检查每日限制
        if not self.daily_limiter.can_make_request_today(data_type, daily_limit):
            logging.info(f"测试: {data_type} 已达到每日请求限制")
            return None

        while not self.rate_limiter.can_make_request(api_name, api_limit // 10):
            wait_time = self.rate_limiter.get_wait_time(api_name, api_limit // 10)
            logging.info(f"等待 {wait_time} 秒以避免 {api_name} 接口速率限制")
            time.sleep(wait_time)

        # 构建分页参数
        kwargs = self.build_test_parameters(data_type, supports)  # 使用测试参数
        kwargs['limit'] = 1000  # 限制单次返回数量

        try:
            df = self.download_with_retry(data_type, kwargs)
            return df
        except Exception as e:
            error_msg = str(e)
            logging.error(f"测试下载 {data_type} 时出错: {error_msg}")
            return None

    def download_daily_update(self, data_type: str):
        """下载单个数据类型的每日更新数据"""
        from metadata import get_last_update_date

        config = DATA_INTERFACE_CONFIG[data_type]
        supports = config['supports']

        # 获取上次更新日期，下载从那时到今天的数据
        last_update = get_last_update_date(data_type)
        if not last_update:
            # 如果没有上次更新记录，使用默认起始日期
            last_update = '20050101'

        # 构建适当的参数
        kwargs = self.build_daily_update_parameters(data_type, supports, last_update)

        # 检查速率限制
        api_limit = config.get('api_limit', 500)  # 默认限制
        api_name = config.get('api_name', data_type)

        while not self.rate_limiter.can_make_request(api_name, api_limit // 10):
            wait_time = self.rate_limiter.get_wait_time(api_name, api_limit // 10)
            logging.info(f"等待 {wait_time} 秒以避免 {api_name} 接口速率限制")
            time.sleep(wait_time)

        # 下载数据
        for attempt in range(3):  # 最多重试3次
            try:
                df = self.download_with_retry(data_type, kwargs)
                return df
            except Exception as e:
                error_msg = str(e)
                if "权限" in error_msg or "速率" in error_msg or "限制" in error_msg:
                    wait_time = 60 * (attempt + 1)
                    logging.warning(f"{data_type} 遇到速率限制，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"下载 {data_type} 时出错: {error_msg}")
                    break

        return None

    def download_daily_update_with_pagination(self, data_type: str):
        """分页下载单个数据类型的每日更新数据（如report_rc）"""
        from metadata import get_last_update_date

        config = DATA_INTERFACE_CONFIG[data_type]
        supports = config['supports']

        # 获取上次更新日期
        last_update = get_last_update_date(data_type)
        if not last_update:
            last_update = '20050101'

        # 检查每日请求限制
        daily_limit = config.get('daily_limit', 10)  # 默认每日10次
        remaining_requests = self.daily_limiter.get_remaining_daily_requests(data_type, daily_limit)
        if remaining_requests <= 0:
            logging.info(f"{data_type} 已达到每日请求限制")
            return None

        # 使用分页下载，充分利用剩余请求次数
        all_data = []
        page = 0
        max_pages = min(remaining_requests, 10)  # 限制最大分页数，防止过度分页

        while len(all_data) < max_pages:
            # 检查速率限制
            api_limit = config.get('api_limit', 500)
            api_name = config.get('api_name', data_type)

            # 检查每日限制
            if not self.daily_limiter.can_make_request_today(data_type, daily_limit):
                logging.info(f"{data_type} 已达到每日请求限制")
                break

            while not self.rate_limiter.can_make_request(api_name, api_limit // 10):
                wait_time = self.rate_limiter.get_wait_time(api_name, api_limit // 10)
                logging.info(f"等待 {wait_time} 秒以避免 {api_name} 接口速率限制")
                time.sleep(wait_time)

            # 构建分页参数
            kwargs = self.build_daily_update_parameters(data_type, supports, last_update)
            kwargs['offset'] = page * 3000  # 偏移量
            kwargs['limit'] = 3000  # 每页最大3000条

            try:
                df = self.download_with_retry(data_type, kwargs)

                if df is None or len(df) == 0:
                    # 如果没有更多数据，停止分页
                    break

                all_data.append(df)

                if len(df) < 3000:
                    # 如果返回的数据少于3000条，说明已到达最后一页
                    break

                page += 1

            except Exception as e:
                error_msg = str(e)
                if "权限" in error_msg or "速率" in error_msg or "限制" in error_msg:
                    wait_time = 60
                    logging.warning(f"{data_type} 遇到速率限制，等待 {wait_time} 秒...")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"下载 {data_type} 时出错: {error_msg}")
                    break

        return all_data

    def build_test_parameters(self, data_type: str, supports: Dict[str, bool]):
        """构建测试参数（使用2019年的数据作为测试示例）"""
        kwargs = {}

        # 设置股票代码（如果需要）
        if supports.get('ts_code'):
            kwargs['ts_code'] = '000001.SZ'

        # 根据接口特点设置参数
        if supports.get('start_date') and supports.get('end_date'):
            kwargs['start_date'] = '20190101'  # 仅在测试场景中使用2019年数据
            kwargs['end_date'] = '20191231'
        elif supports.get('trade_date'):
            kwargs['trade_date'] = '20190101'
        elif supports.get('ann_date'):
            kwargs['ann_date'] = '20190101'
        elif supports.get('period'):
            kwargs['period'] = '20190331'
        elif supports.get('month'):
            kwargs['month'] = '201901'
        elif supports.get('exchange'):
            kwargs['exchange'] = 'SSE'
        else:
            kwargs['start_date'] = '20190101'
            kwargs['end_date'] = '20190131'

        # 特殊接口处理
        if data_type == 'daily':
            kwargs['adj'] = 'hfq'
        elif data_type == 'broker_recommend':
            kwargs['month'] = '201901'

        return kwargs

    def build_daily_update_parameters(self, data_type: str, supports: Dict[str, bool], last_update: str):
        """构建每日更新参数"""
        kwargs = {}

        # 设置股票代码（如果需要）
        if supports.get('ts_code'):
            kwargs['ts_code'] = '000001.SZ'

        # 根据接口特点设置参数
        if supports.get('start_date') and supports.get('end_date'):
            kwargs['start_date'] = last_update
            kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
        elif supports.get('trade_date'):
            kwargs['trade_date'] = last_update
        elif supports.get('ann_date'):
            kwargs['ann_date'] = last_update
        elif supports.get('period'):
            # 对于财务数据，可能需要调整季度
            kwargs['period'] = last_update[:6] + '31'  # 使用上次更新日期的季度
        elif supports.get('month'):
            kwargs['month'] = last_update[:6]  # 使用上次更新日期的月份
        elif supports.get('exchange'):
            kwargs['exchange'] = 'SSE'
        else:
            kwargs['start_date'] = last_update
            kwargs['end_date'] = datetime.now().strftime('%Y%m%d')

        # 特殊接口处理
        if data_type == 'daily':
            kwargs['adj'] = 'hfq'
        elif data_type == 'broker_recommend':
            kwargs['month'] = last_update[:6]

        return kwargs

    def process_and_store_data(self, data_type: str, df):
        """处理和存储数据"""
        if df is not None and len(df) > 0:
            try:
                # 在独立线程中运行ETL处理
                self.executor.submit(self._run_etl_process, data_type, df)
                logging.info(f"{data_type} 数据已提交存储处理")
            except Exception as e:
                logging.error(f"提交处理 {data_type} 数据时出错: {str(e)}")

    def _run_etl_process(self, data_type: str, df):
        """运行ETL处理"""
        try:
            EtlRuntime.process_data(data_type, df=df)
            logging.info(f"{data_type} 数据已存储到指定位置")
        except Exception as e:
            logging.error(f"ETL处理 {data_type} 时出错: {str(e)}")

    def download_all_data_test(self):
        """多线程下载所有数据类型的测试数据"""
        futures = []
        for data_type in DATA_INTERFACE_CONFIG.keys():
            # 检查是否是每日请求限制接口（如report_rc）
            config = DATA_INTERFACE_CONFIG[data_type]
            daily_limit = config.get('daily_limit', None)

            if daily_limit is not None:
                # 对于有每日限制的接口，检查是否还有当日请求次数
                # 但测试时可能需要特殊处理，因为可能已经用完了每日限制
                remaining = self.daily_limiter.get_remaining_daily_requests(data_type, daily_limit)

                # 为了测试，我们对有每日限制的接口采用特殊处理
                # 实际上在测试场景中，我们会使用相同的参数，但需要注意可能遇到限制
                if remaining <= 0:
                    logging.info(f"测试: {data_type} 已达到每日请求限制，跳过测试")
                    continue

            # 提交下载任务到线程池
            future = self.executor.submit(self.download_single_data_type, data_type)
            futures.append((data_type, future))

        # 处理结果并存储
        for data_type, future in futures:
            try:
                result = future.result()  # 等待任务完成
                if result is not None:
                    self.process_and_store_data(data_type, result)
            except Exception as e:
                logging.error(f"处理 {data_type} 时出错: {str(e)}")

    def download_all_data_daily_update(self):
        """多线程下载所有数据类型的每日更新数据"""
        futures = []
        for data_type in DATA_INTERFACE_CONFIG.keys():
            # 检查是否是每日请求限制接口（如report_rc）
            config = DATA_INTERFACE_CONFIG[data_type]
            daily_limit = config.get('daily_limit', None)

            if daily_limit is not None:
                # 检查是否还有当日请求次数
                remaining = self.daily_limiter.get_remaining_daily_requests(data_type, daily_limit)
                if remaining <= 0:
                    logging.info(f"{data_type} 已达到每日请求限制，跳过今日更新")
                    continue
                # 使用分页下载以充分利用当日剩余请求次数
                future = self.executor.submit(self.download_daily_update_with_pagination, data_type)
            else:
                # 普通接口按速率限制下载
                future = self.executor.submit(self.download_daily_update, data_type)

            futures.append((data_type, future))

        # 处理结果并存储
        for data_type, future in futures:
            try:
                result = future.result()  # 等待任务完成
                if result is not None:
                    if isinstance(result, list):
                        # 分页下载返回多个数据框，需要合并
                        for i, df in enumerate(result):
                            if df is not None and len(df) > 0:
                                # 为分页数据添加标识
                                self.process_and_store_data(f"{data_type}_page_{i}", df)
                    else:
                        # 单次下载返回单个数据框
                        if result is not None and len(result) > 0:
                            self.process_and_store_data(data_type, result)
            except Exception as e:
                logging.error(f"处理 {data_type} 时出错: {str(e)}")

    def close(self):
        """关闭线程池"""
        self.executor.shutdown(wait=True)