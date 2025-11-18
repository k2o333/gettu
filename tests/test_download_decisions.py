#!/usr/bin/env python3
"""
Test script for enhanced scanner decision engine
"""
import sys
import os
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

import logging
from pathlib import Path
from enhanced_scanner import DownloadDecisionEngine

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

    # 模拟扫描结果（有数据的情况）
    scan_results = {
        'daily': {
            'has_data': True,
            'stock_coverage': {
                '000001.SZ': {
                    'date_range': ('20250601', '20251115'),
                    'record_count': 100,
                    'integrity': {
                        'is_complete': True,
                        'issues': []
                    }
                }
            },
            'integrity_issues': 0
        }
    }

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

    # 测试无数据情况
    print("\n=== 测试无数据情况 ===")
    empty_scan_results = {
        'daily': {
            'has_data': False,
            'stock_coverage': {},
            'integrity_issues': 0
        }
    }

    empty_decisions = decision_engine.make_download_decisions(
        empty_scan_results, '20250601', '20251116'
    )

    for data_type, decision in empty_decisions.items():
        print(f"数据类型: {data_type}")
        print(f"  策略: {decision['strategy']} (原因: {decision.get('reason', 'N/A')})")

        if decision['strategy'] == 'full_download':
            print("  ✓ 完整下载策略正确（无数据情况）")

if __name__ == "__main__":
    test_download_decisions()