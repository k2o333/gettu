#!/usr/bin/env python3
"""
Test script for dictionary persistence fix
"""
import sys
import os
sys.path.insert(0, '/home/quan/testdata/aspipe/app2')

import logging
from pathlib import Path
from config import DICT_DIR
from dictionaries import dictionaries_exist, create_dictionaries

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def test_dictionary_persistence():
    """测试字典持久化功能"""
    setup_logging()

    print("=== 测试字典持久化功能 ===")

    # 检查字典是否存在
    exists_before = dictionaries_exist()
    print(f"修复前字典存在: {exists_before}")

    if exists_before:
        # 获取修改时间作为参考
        dict_files = [
            DICT_DIR / 'stock_basic_dict.parquet',
            DICT_DIR / 'industry_dict.parquet',
            DICT_DIR / 'area_dict.parquet'
        ]

        mod_times_before = {}
        for file_path in dict_files:
            if file_path.exists():
                mod_times_before[file_path.name] = file_path.stat().st_mtime
                print(f"  {file_path.name} 修改时间: {mod_times_before[file_path.name]}")

    # 尝试创建字典（应该跳过）
    print("\n尝试创建字典（应该跳过）...")
    create_dictionaries()

    # 检查字典状态
    exists_after = dictionaries_exist()
    print(f"修复后字典存在: {exists_after}")

    if exists_after and exists_before:
        # 检查修改时间是否变化
        dict_files = [
            DICT_DIR / 'stock_basic_dict.parquet',
            DICT_DIR / 'industry_dict.parquet',
            DICT_DIR / 'area_dict.parquet'
        ]

        all_unchanged = True
        for file_path in dict_files:
            if file_path.exists():
                current_mtime = file_path.stat().st_mtime
                print(f"  {file_path.name} 当前修改时间: {current_mtime}")
                if file_path.name in mod_times_before:
                    if current_mtime != mod_times_before[file_path.name]:
                        print(f"    警告: {file_path.name} 修改时间已变化！")
                        all_unchanged = False
                    else:
                        print(f"    {file_path.name} 修改时间未变化（正确）")

        if all_unchanged:
            print("\n✓ 字典持久化测试通过：字典文件未被不必要地重新创建")
        else:
            print("\n✗ 字典持久化测试失败：字典文件被不必要地重新创建")

    # 测试强制重新创建
    print("\n=== 测试强制重新创建字典 ===")
    print("强制创建字典...")
    create_dictionaries(force_recreate=True)
    print("✓ 强制创建完成")

if __name__ == "__main__":
    test_dictionary_persistence()