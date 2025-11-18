#!/usr/bin/env python3
"""
Debug script for data scanning
"""
import sys
import os
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

# Set up environment
os.environ['TUSHARE_TOKEN'] = 'test_token'

import logging
from pathlib import Path
from enhanced_scanner import DataScanner

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def test_data_scanning():
    """测试数据扫描功能"""
    setup_logging()

    print("=== 调试数据扫描功能 ===")

    # 初始化扫描器
    scanner = DataScanner()

    # 手动创建配置
    config = {
        'storage': {
            'path': Path('/home/quan/testdata/aspipe/data/daily/daily_hfq'),
            'partition_granularity': 'year'
        }
    }

    # 扫描特定数据类型
    print("扫描daily数据类型...")
    result = scanner.scan_data_type('daily', config)

    print(f"扫描结果: {result}")

if __name__ == "__main__":
    test_data_scanning()