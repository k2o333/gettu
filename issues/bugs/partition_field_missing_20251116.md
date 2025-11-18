# Bug Report: Missing Partition Field in ETL Processing

**Date**: 2025-11-16
**Issue**: "无法找到 'year' 分区字段" (Cannot find 'year' partition field) error during ETL processing
**Component**: ETL Runtime Processing Pipeline
**Severity**: Critical

## Bug Description

During the enhanced initial data build process, the system successfully downloads daily stock data from TuShare API (showing "daily数据下载完成: 113 条记录" in logs), but fails during ETL processing with the error:

```
ERROR:root:daily: 无法找到 'year' 分区字段，立即中断执行。请检查 trade_date 字段是否存在。
WARNING:root:下载daily股票000001.SZ数据失败: 配置为年分区存储但分区字段 'year' 不存在: daily
```

## Root Cause Analysis

The issue occurs in the ETL processing pipeline in `etl_runtime.py`:

1. **Data Download**: TuShare API correctly returns data with `trade_date` column (as confirmed in TuShare documentation)
2. **ETL Entry Point**: `EtlRuntime.process_data()` is called with the downloaded DataFrame
3. **Field Normalization**: `_normalize_fields()` method attempts to create partition fields:
   - Line 166: `if partition_field and partition_field in available_columns:`
   - `partition_field` is `'trade_date'` (from config for daily data type)
   - `available_columns` should contain `'trade_date'` but apparently doesn't
4. **Partition Field Creation**: Because the condition evaluates to false, the `year` field is never created
5. **Write Data**: `_write_data()` method fails when checking for the missing `year` field

## Specific Code Path

1. `main.py` → `run_initial_build_enhanced()`
2. `enhanced_scanner.py` → `build_with_enhanced_scan()`
3. `custom_build.py` → `_build_by_stock_with_date_range()`
4. `interface_manager.py` → `download_data_by_config()` → Successfully downloads data
5. `etl_runtime.py` → `EtlRuntime.process_data()` → **FAILS HERE**
6. `etl_runtime.py` → `_normalize_fields()` → Condition fails, `year` field not created
7. `etl_runtime.py` → `_write_data()` → Checks for missing `year` field → **ERROR THROWN**

## Likely Causes

1. **Field Mapping Issue**: The field mapping logic in `_normalize_fields()` (lines 157-160) may be incorrectly renaming or removing the `trade_date` column
2. **Schema Access Issue**: There may be an issue with `lazy_frame.collect_schema().names()` not properly detecting columns
3. **Data Conversion Issue**: The conversion from pandas DataFrame to Polars DataFrame may be causing column name issues
4. **Timing Issue**: The `trade_date` column may be getting removed or renamed between download and ETL processing

## Evidence from Logs

- Data downloads successfully: "daily数据下载完成: 113 条记录"
- ETL processing fails: "daily: 无法找到 'year' 分区字段，立即中断执行。请检查 trade_date 字段是否存在。"
- Performance warnings suggest schema access issues: warnings about expensive LazyFrame schema operations

## Configuration Details

- Data type: `daily`
- Partition granularity: `year` (from `PartitionGranularity.YEAR`)
- Partition field: `trade_date` (from config)
- Expected behavior: Create `year` field from `trade_date` for yearly partitioning

## Impact

This bug prevents any daily stock data from being processed and stored with yearly partitioning, blocking the initial data build process.

## Solution Direction

The fix would involve ensuring that the `trade_date` column is properly detected and available when `_normalize_fields()` attempts to create partition fields from it.