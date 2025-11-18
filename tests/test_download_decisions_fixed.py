#!/usr/bin/env python3
"""
Test script for enhanced scanner decision engine with proper config
"""
import sys
import os
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

# Set up environment
os.environ['TUSHARE_TOKEN'] = 'test_token'

import logging
from pathlib import Path
from enhanced_scanner import DownloadDecisionEngine, DataScanner

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def test_download_decisions():
    """测试下载决策功能"""
    setup_logging()

    print("=== 测试下载决策功能 ===")

    # 初始化扫描器
    scanner = DataScanner()
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

    # 初始化决策引擎
    decision_engine = DownloadDecisionEngine()

    # 生成下载决策
    print("生成下载决策...")
    decisions = decision_engine.make_download_decisions(
        scan_results, '20250601', '20251116'
    )

    print(f"决策结果: {list(decisions.keys())}")

    for data_type, decision in decisions.items():
        print(f"\n数据类型: {data_type}")
        print(f"  策略: {decision['strategy']}")

        if decision['strategy'] == 'incremental':
            print("  ✓ 增量下载策略正确")
            stock_decisions = decision.get('stock_decisions', {})
            print(f"  股票决策数: {len(stock_decisions)}")

            for ts_code, stock_decision in stock_decisions.items():
                print(f"    {ts_code}: {stock_decision['action']} - {stock_decision['reason']}")
                if 'missing_periods' in stock_decision:
                    print(f"      缺失时间段: {stock_decision['missing_periods']}")
        else:
            print(f"  策略: {decision['strategy']} (原因: {decision.get('reason', 'N/A')})")

    # Test a case where there should be missing data at the end
    print("\n=== 分析决策逻辑 ===")
    for data_type, decision in decisions.items():
        if decision['strategy'] == 'incremental':
            for ts_code, stock_decision in decision['stock_decisions'].items():
                print(f"{ts_code} 现有范围: {stock_decision.get('existing_range', 'N/A')}")
                print(f"{ts_code} 决策: {stock_decision['action']} ({stock_decision['reason']})")
                if 'missing_periods' in stock_decision:
                    print(f"  需要补全时间段: {stock_decision['missing_periods']}")

if __name__ == "__main__":
    test_download_decisions()