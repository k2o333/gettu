#!/usr/bin/env python3
"""
Test script for incremental download fix with proper config
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
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def test_data_scanning():
    """测试数据扫描功能"""
    setup_logging()

    print("=== 测试数据扫描功能 ===")

    # 初始化扫描器
    scanner = DataScanner()

    # 手动 create config for daily
    from config import DAILY_DIR
    config = {
        'storage': {
            'path': DAILY_DIR / 'daily_hfq',
            'partition_granularity': 'year'
        }
    }

    # 扫描现有数据
    print("扫描现有数据...")
    scan_results = {'daily': scanner.scan_data_type('daily', config)}

    print(f"扫描结果: {list(scan_results.keys())}")

    for data_type, result in scan_results.items():
        print(f"\n数据类型: {data_type}")
        print(f"  有数据: {result['has_data']}")
        print(f"  文件数: {result['total_files']}")
        print(f"  记录数: {result['total_records']}")
        print(f"  完整性问题: {result['integrity_issues']}")
        print(f"  股票覆盖数: {len(result['stock_coverage'])}")

        if result['has_data']:
            print("  ✓ 数据扫描测试通过：正确识别到现有数据")
        else:
            print("  ✗ 数据扫描测试失败：未识别到现有数据")

        # 显示一些股票覆盖信息
        if result['stock_coverage']:
            print("  股票覆盖示例:")
            count = 0
            for ts_code, coverage in result['stock_coverage'].items():
                print(f"    {ts_code}: {coverage['record_count']} 条记录, 日期范围: {coverage['date_range']}")
                count += 1
                if count >= 3:  # 只显示前3个
                    break

if __name__ == "__main__":
    test_data_scanning()