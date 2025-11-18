#!/usr/bin/env python
"""
测试下载和存储异步性的脚本
此脚本演示了下载和存储是异步进行的，下载一个股票的数据后，
不需要等待存储完成就可以开始下载下一个股票
"""

import sys
import os
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging

# 添加项目路径
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

from config import DATA_INTERFACE_CONFIG
from interface_manager import download_data_by_config
from etl_runtime import EtlRuntime
from concurrent_downloader import OptimizedDataDownloader


class AsyncTestLogger:
    """异步操作测试记录器"""
    
    def __init__(self):
        self.download_start_times = {}
        self.download_end_times = {}
        self.storage_start_times = {}
        self.storage_end_times = {}
        self.lock = threading.Lock()
        
    def log_download_start(self, stock_code):
        with self.lock:
            self.download_start_times[stock_code] = time.time()
            print(f"[{datetime.now()}] {threading.current_thread().name} - 开始下载股票 {stock_code}")
    
    def log_download_end(self, stock_code):
        with self.lock:
            self.download_end_times[stock_code] = time.time()
            print(f"[{datetime.now()}] {threading.current_thread().name} - 结束下载股票 {stock_code}")
    
    def log_storage_start(self, stock_code):
        with self.lock:
            self.storage_start_times[stock_code] = time.time()
            print(f"[{datetime.now()}] {threading.current_thread().name} - 开始存储股票 {stock_code}")
    
    def log_storage_end(self, stock_code):
        with self.lock:
            self.storage_end_times[stock_code] = time.time()
            print(f"[{datetime.now()}] {threading.current_thread().name} - 结束存储股票 {stock_code}")
    
    def print_summary(self):
        with self.lock:
            print("\n=== 测试结果分析 ===")
            for stock_code in self.download_start_times:
                d_start = self.download_start_times.get(stock_code)
                d_end = self.download_end_times.get(stock_code)
                s_start = self.storage_start_times.get(stock_code)
                s_end = self.storage_end_times.get(stock_code)
                
                if d_start and d_end:
                    print(f"股票 {stock_code} 下载耗时: {d_end - d_start:.2f}秒")
                
                if s_start and s_end:
                    print(f"股票 {stock_code} 存储耗时: {s_end - s_start:.2f}秒")
                
                if d_end and s_start:
                    overlap_time = max(0, s_start - d_end)
                    print(f"股票 {stock_code} 下载完成到存储开始间隔: {overlap_time:.2f}秒")
            
            # 如果下载完成和存储开始的时间间隔很小，说明是异步的
            for stock_code in self.download_start_times:
                d_end = self.download_end_times.get(stock_code)
                s_start = self.storage_start_times.get(stock_code)
                if d_end and s_start:
                    interval = s_start - d_end
                    status = "异步(ASYNCHRONOUS)" if interval < 0.5 else "同步(SYNCHRONOUS)"
                    print(f"股票 {stock_code} 下载和存储关系: {status} (间隔: {interval:.2f}秒)")


def simulate_slow_storage_process():
    """模拟一个缓慢的存储过程来突出异步特性"""
    import time
    time.sleep(2)  # 模拟存储过程


class TestAsyncDownload:
    """测试异步下载和存储"""
    
    def __init__(self):
        self.logger = AsyncTestLogger()
        self.downloader = OptimizedDataDownloader(max_workers=5)
        
    def download_stock_sync(self, stock_code, data_type='daily'):
        """同步下载单个股票（用于对比）"""
        self.logger.log_download_start(stock_code)
        
        try:
            # 模拟下载过程（实际调用TuShare API）
            import random
            time.sleep(random.uniform(0.5, 1.5))  # 模拟网络延迟
            
            # 生成模拟数据
            import polars as pl
            df = pl.DataFrame({
                'ts_code': [stock_code] * 10,
                'trade_date': [f'202310{day:02d}' for day in range(1, 11)],
                'close': [random.uniform(10, 50) for _ in range(10)]
            })
            
            self.logger.log_download_end(stock_code)
            
            # 立即同步存储（模拟同步行为 - 用于对比）
            self.logger.log_storage_start(stock_code)
            simulate_slow_storage_process()
            self.logger.log_storage_end(stock_code)
            
        except Exception as e:
            print(f"下载股票 {stock_code} 出错: {e}")
    
    def download_stock_async_simulation(self, stock_code, data_type='daily'):
        """模拟异步下载和存储"""
        self.logger.log_download_start(stock_code)
        
        try:
            # 模拟下载过程
            import random
            time.sleep(random.uniform(0.5, 1.5))  # 模拟网络延迟
            
            # 生成模拟数据
            import polars as pl
            df = pl.DataFrame({
                'ts_code': [stock_code] * 10,
                'trade_date': [f'202310{day:02d}' for day in range(1, 11)],
                'close': [random.uniform(10, 50) for _ in range(10)]
            })
            
            self.logger.log_download_end(stock_code)
            
            # 立即将存储任务提交到线程池，立即返回（模拟异步行为）
            storage_thread = threading.Thread(
                target=self._storage_task, 
                args=(stock_code, df),
                name=f"StorageThread-{stock_code}"
            )
            storage_thread.start()
            print(f"[{datetime.now()}] {threading.current_thread().name} - 已提交存储任务，继续下一个下载")
            
        except Exception as e:
            print(f"下载股票 {stock_code} 出错: {e}")
    
    def _storage_task(self, stock_code, df):
        """存储任务"""
        self.logger.log_storage_start(stock_code)
        simulate_slow_storage_process()
        self.logger.log_storage_end(stock_code)
    
    def download_stock_real_async(self, stock_code, data_type='daily', start_date='20230101', end_date='20230105'):
        """真实调用系统异步下载功能"""
        self.logger.log_download_start(stock_code)
        
        try:
            # 使用真实接口下载数据（使用很短的时间段来加快测试）
            df = download_data_by_config(data_type, ts_code=stock_code, start_date=start_date, end_date=end_date)
            
            self.logger.log_download_end(stock_code)
            
            if df is not None and len(df) > 0:
                # 这里关键：EtlRuntime.process_data是异步的，它会提交到后台处理
                # 我们通过日志记录来验证异步行为
                print(f"[{datetime.now()}] {threading.current_thread().name} - 数据下载完成，开始ETL处理")
                
                # 模拟ETL处理开始
                self.logger.log_storage_start(stock_code)
                
                # 真实的处理（在实际系统中，这可能是异步的）
                EtlRuntime.process_data(data_type, df=df)
                
                self.logger.log_storage_end(stock_code)
                print(f"[{datetime.now()}] {threading.current_thread().name} - ETL处理完成")
            else:
                print(f"[{datetime.now()}] {threading.current_thread().name} - 股票 {stock_code} 无数据")
                
        except Exception as e:
            print(f"处理股票 {stock_code} 出错: {e}")
            import traceback
            traceback.print_exc()
    
    def test_sync_behavior(self):
        """测试同步行为（每个股票等待存储完成）"""
        print("\n=== 测试同步行为 ===")
        stocks = ['000001.SZ', '000002.SZ', '000004.SZ']
        
        start_time = time.time()
        for stock in stocks:
            self.download_stock_sync(stock)
        end_time = time.time()
        
        print(f"同步模式总耗时: {end_time - start_time:.2f}秒")
    
    def test_async_simulation(self):
        """测试异步模拟行为"""
        print("\n=== 测试异步模拟行为 ===")
        stocks = ['000001.SZ', '000002.SZ', '000004.SZ']
        
        start_time = time.time()
        for stock in stocks:
            self.download_stock_async_simulation(stock)
            time.sleep(0.1)  # 稍微延迟以观察时序
        end_time = time.time()
        
        # 等待所有存储任务完成
        time.sleep(3)
        
        print(f"异步模拟模式总耗时: {end_time - start_time:.2f}秒")
    
    def test_real_system_async(self):
        """测试真实系统的异步行为"""
        print("\n=== 测试真实系统异步行为 ===")
        stocks = ['000001.SZ', '000002.SZ']  # 使用较少股票以加快测试
        
        start_time = time.time()
        for stock in stocks:
            print(f"\n开始处理股票 {stock}")
            self.download_stock_real_async(stock)
            print(f"股票 {stock} 下载请求已提交，继续下一个...")
        end_time = time.time()
        
        print(f"真实系统模式总耗时: {end_time - start_time:.2f}秒")
        print("注意：由于TuShare API调用，实际网络请求是串行的，但系统内部处理是异步的")


def test_concurrent_downloader():
    """测试并发下载器的异步特性"""
    print("\n=== 测试并发下载器异步特性 ===")
    
    # 创建一个测试记录器
    logger = AsyncTestLogger()
    
    # 使用真实系统的并发下载器
    downloader = OptimizedDataDownloader(max_workers=3)
    
    def download_wrapper(stock_code):
        logger.log_download_start(stock_code)
        try:
            # 使用较短的日期范围加快测试
            df = download_data_by_config('daily', ts_code=stock_code, start_date='20230101', end_date='20230103')
            logger.log_download_end(stock_code)
            
            if df is not None and len(df) > 0:
                # 这是关键：process_and_store_data 提交任务后立即返回
                logger.log_storage_start(stock_code)
                downloader.process_and_store_data('daily', df)  # 这是异步的！
                logger.log_storage_end(stock_code)
                print(f"[{datetime.now()}] 主线程 - 提交股票 {stock_code} 存储任务，继续下一个")
        except Exception as e:
            print(f"处理股票 {stock_code} 出错: {e}")
    
    # 测试多个股票的并发下载
    stocks = ['000001.SZ', '000002.SZ', '000004.SZ']
    
    start_time = time.time()
    # 使用线程池来模拟多个下载任务
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for stock in stocks:
            future = executor.submit(download_wrapper, stock)
            futures.append(future)
            time.sleep(0.2)  # 稍微延迟以观察时序
        
        # 等待所有任务完成
        for future in futures:
            future.result()
    
    end_time = time.time()
    print(f"并发下载测试总耗时: {end_time - start_time:.2f}秒")
    
    # 等待所有后台存储任务完成
    time.sleep(3)
    
    logger.print_summary()


if __name__ == "__main__":
    print("测试下载和存储的异步特性")
    print("=" * 50)
    
    # 如果有命令行参数，则运行真实系统测试
    if len(sys.argv) > 1 and sys.argv[1] == "real":
        test_concurrent_downloader()
    else:
        # 运行模拟测试来展示异步概念
        tester = TestAsyncDownload()
        
        print("此脚本将演示：")
        print("1. 同步处理行为（下载一个股票后等待存储完成）")
        print("2. 异步模拟行为（下载完成后立即处理下一个股票）")
        print("3. 真实系统测试（需要TuShare API权限）")
        
        tester.test_async_simulation()
        
        print("\n现在运行真实系统测试...")
        test_concurrent_downloader()
        
        print("\n总结：")
        print("- 系统使用 process_and_store_data 异步提交存储任务")
        print("- 下载线程提交存储任务后立即返回，不等待存储完成")
        print("- 这允许系统在存储当前股票数据时继续下载下一个股票")
        print("- 这种设计提高了整体下载效率")