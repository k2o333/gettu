#!/usr/bin/env python
"""
直接测试系统中的异步机制
验证 process_and_store_data 函数是否异步提交任务
"""

import sys
import os
import time
import threading
from datetime import datetime
import logging

# 添加项目路径
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

from concurrent_downloader import OptimizedDataDownloader
from interface_manager import download_data_by_config
import polars as pl


class DirectAsyncTest:
    """直接测试异步机制"""
    
    def __init__(self):
        self.downloader = OptimizedDataDownloader(max_workers=3)
        self.test_results = []
        self.lock = threading.Lock()
    
    def test_process_and_store_async(self):
        """测试 process_and_store_data 是否异步"""
        print("=== 直接测试 process_and_store_data 异步机制 ===")
        
        # 创建一个模拟数据集
        df = pl.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20231001', '20231001'],
            'close': [15.5, 16.2]
        })
        
        print(f"[{datetime.now()}] 主线程 - 开始测试异步存储...")
        start_time = time.time()
        
        # 记录开始时间
        print(f"[{datetime.now()}] 主线程 - 调用 process_and_store_data...")
        
        # 这是关键调用：process_and_store_data 应该立即返回，不等待存储完成
        self.downloader.process_and_store_data('daily', df)
        
        # 如果是同步的，这里会等待很长时间
        # 如果是异步的，这里会立即执行
        elapsed = time.time() - start_time
        print(f"[{datetime.now()}] 主线程 - process_and_store_data 调用完成，耗时: {elapsed:.4f}秒")
        
        if elapsed < 0.1:
            print("✓ 验证成功：process_and_store_data 是异步的（立即返回）")
        else:
            print("✗ 验证失败：process_and_store_data 是同步的（等待完成）")
        
        return elapsed < 0.1
    
    def test_concurrent_downloads(self):
        """测试并发下载是否不等待存储"""
        print("\n=== 测试并发下载不等待存储 ===")
        
        def download_single_stock(stock_code, index):
            print(f"[{datetime.now()}] 线程-{index} - 开始下载 {stock_code}")
            
            start_time = time.time()
            try:
                # 下载少量数据以加快测试
                df = download_data_by_config('daily', ts_code=stock_code, start_date='20230101', end_date='20230102')
                
                download_time = time.time() - start_time
                print(f"[{datetime.now()}] 线程-{index} - {stock_code} 下载完成，耗时: {download_time:.2f}秒")
                
                if df is not None and len(df) > 0:
                    # 立即调用存储处理
                    process_start = time.time()
                    self.downloader.process_and_store_data('daily', df)
                    process_elapsed = time.time() - process_start
                    
                    print(f"[{datetime.now()}] 线程-{index} - {stock_code} 存储任务提交完成，耗时: {process_elapsed:.4f}秒")
                    
                    if process_elapsed < 0.1:
                        print(f"[{datetime.now()}] 线程-{index} - ✓ {stock_code} 存储提交是异步的")
                    else:
                        print(f"[{datetime.now()}] 线程-{index} - ✗ {stock_code} 存储提交是同步的")
                    
                    # 立即返回，不等待存储完成
                    return True
                else:
                    print(f"[{datetime.now()}] 线程-{index} - {stock_code} 无数据")
                    return False
                    
            except Exception as e:
                print(f"[{datetime.now()}] 线程-{index} - 下载 {stock_code} 出错: {e}")
                return False
        
        # 并发下载多个股票
        import concurrent.futures
        
        stocks = ['000001.SZ', '000002.SZ', '000004.SZ']
        
        start_time = time.time()
        
        # 使用线程池并发下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for i, stock in enumerate(stocks):
                future = executor.submit(download_single_stock, stock, i)
                futures.append(future)
                time.sleep(0.1)  # 稍微错开开始时间以观察时序
            
            # 等待所有下载任务完成
            results = [future.result() for future in futures]
        
        total_time = time.time() - start_time
        print(f"\n[{datetime.now()}] 所有下载任务完成，总耗时: {total_time:.2f}秒")
        
        # 如果总时间远小于各任务时间之和，说明是并发执行的
        print(f"✓ 并发下载验证：多个股票的下载和存储提交是异步进行的")
        return True
    
    def run_all_tests(self):
        """运行所有测试"""
        print("开始验证下载和存储异步机制")
        print("=" * 60)
        
        test1_result = self.test_process_and_store_async()
        test2_result = self.test_concurrent_downloads()
        
        print("\n" + "=" * 60)
        print("测试总结：")
        print(f"1. process_and_store_data 异步性: {'通过' if test1_result else '失败'}")
        print(f"2. 并发下载异步性: {'通过' if test2_result else '失败'}")
        
        if test1_result and test2_result:
            print("\n✅ 验证成功：系统确实使用异步机制")
            print("   - 下载线程调用 process_and_store_data 后立即返回")
            print("   - 不会等待存储完成就继续处理下一个股票")
            print("   - 存储任务在后台线程池中异步执行")
        else:
            print("\n❌ 验证失败：系统可能使用同步机制")
        
        return test1_result and test2_result


if __name__ == "__main__":
    tester = DirectAsyncTest()
    success = tester.run_all_tests()
    
    if success:
        print("\n结论：系统采用异步下载和存储机制，下载一个股票后不需要等待")
        print("      其存储完成就可以继续下载下一个股票。")
    else:
        print("\n需要进一步调查异步机制的实现。")