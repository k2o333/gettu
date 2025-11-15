#!/usr/bin/env python3
"""
数据完整性检查脚本
验证 config.py 中配置的数据是否完整
"""
import polars as pl
import logging
from pathlib import Path
from datetime import datetime
from config import DATA_INTERFACE_CONFIG, ROOT_DIR, PartitionGranularity


def setup_logging():
    """设置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def check_data_integrity(data_type=None):
    """检查指定数据类型的数据完整性"""
    if data_type:
        if data_type not in DATA_INTERFACE_CONFIG:
            print(f"错误: 数据类型 '{data_type}' 不存在于配置中")
            return False
        data_types_to_check = [data_type]
    else:
        data_types_to_check = list(DATA_INTERFACE_CONFIG.keys())

    print(f"开始检查 {len(data_types_to_check)} 个数据类型的完整性...")
    print("=" * 80)
    
    all_checks_passed = True
    
    for dt in data_types_to_check:
        config = DATA_INTERFACE_CONFIG[dt]
        storage_path = config['storage']['path']
        partition_granularity = config['storage']['partition_granularity']
        
        print(f"\n检查数据类型: {dt}")
        print(f"  存储路径: {storage_path}")
        print(f"  分区粒度: {partition_granularity}")
        
        try:
            # 检查数据是否存在
            if partition_granularity != PartitionGranularity.NONE:
                # 分区存储 - 但实际检查时也要考虑可能存储为非分区文件的情况
                if storage_path.exists():
                    # 检查是否直接是.parquet文件（非分区存储）
                    parquet_files = list(storage_path.glob("**/*.parquet"))
                    parquet_files = [f for f in parquet_files if f.name.endswith('.parquet')]

                    # 同时检查分区格式（如 year=2023/data.parquet）
                    partition_files = list(storage_path.glob("**/data.parquet"))

                    if partition_files:  # 有分区格式文件
                        data_files = partition_files
                        storage_type = "分区格式"
                    elif parquet_files:  # 有直接的.parquet文件
                        data_files = parquet_files
                        storage_type = "非分区文件"
                    else:  # 都没有
                        print(f"  ❌ 未找到数据文件 (期望分区存储)")
                        all_checks_passed = False
                        continue

                    total_records = 0
                    for file_path in data_files:
                        try:
                            df = pl.read_parquet(file_path)
                            total_records += len(df)
                        except Exception as e:
                            print(f"  ❌ 读取文件失败 {file_path}: {str(e)}")
                            all_checks_passed = False
                            continue

                    print(f"  ✅ 找到 {len(data_files)} 个{storage_type}，总计 {total_records} 条记录")

            else:
                # 非分区存储
                if storage_path.suffix != '.parquet':
                    storage_path = Path(f"{storage_path}.parquet")

                if not storage_path.exists():
                    print(f"  ❌ 文件不存在: {storage_path}")
                    all_checks_passed = False
                    continue

                try:
                    df = pl.read_parquet(storage_path)
                    print(f"  ✅ 文件存在，记录数: {len(df)}")

                    # 检查关键字段是否存在
                    partition_field = config['storage']['partition_field']
                    if partition_field and partition_field in df.columns:
                        print(f"  ✅ 分区字段 '{partition_field}' 存在，有 {df[partition_field].n_unique()} 个唯一值")

                    # 显示数据日期范围（如果有日期字段）
                    date_fields = [col for col in df.columns if 'date' in col.lower()]
                    if date_fields:
                        for date_field in date_fields[:3]:  # 最多显示3个日期字段
                            try:
                                date_min = df[date_field].min()
                                date_max = df[date_field].max()
                                print(f"  📅 {date_field} 范围: {date_min} ~ {date_max}")
                            except:
                                continue

                except Exception as e:
                    print(f"  ❌ 读取文件失败 {storage_path}: {str(e)}")
                    all_checks_passed = False
                    continue
        
        except Exception as e:
            print(f"  ❌ 检查过程中出错: {str(e)}")
            all_checks_passed = False
        
        print("-" * 60)
    
    print(f"\n完整性检查完成!")
    print(f"总体结果: {'✅ 全部通过' if all_checks_passed else '❌ 存在问题'}")
    return all_checks_passed


def check_data_coverage_by_date_range():
    """检查数据的时间覆盖范围"""
    print("\n" + "=" * 80)
    print("数据时间覆盖范围检查")
    print("=" * 80)
    
    date_coverage = {}
    
    for dt, config in DATA_INTERFACE_CONFIG.items():
        storage_path = config['storage']['path']
        partition_granularity = config['storage']['partition_granularity']
        
        # 确保路径是 .parquet 文件
        if partition_granularity == PartitionGranularity.NONE and storage_path.suffix != '.parquet':
            storage_path = Path(f"{storage_path}.parquet")
        
        try:
            if partition_granularity != PartitionGranularity.NONE:
                # 分区存储 - 需要查找所有分区中的数据
                if partition_granularity == PartitionGranularity.YEAR:
                    data_files = list(storage_path.glob("**/data.parquet"))
                elif partition_granularity == PartitionGranularity.YEAR_MONTH:
                    data_files = list(storage_path.glob("**/data.parquet"))
                else:
                    continue
                
                all_dates = []
                date_field = config['storage']['partition_field']
                
                for file_path in data_files:
                    try:
                        df = pl.read_parquet(file_path)
                        if date_field and date_field in df.columns:
                            dates = df[date_field].drop_nulls().cast(pl.Utf8).to_list()
                            all_dates.extend(dates)
                    except:
                        continue
                
                if all_dates:
                    all_dates = [d.replace('-', '').replace('/', '') for d in all_dates if d]
                    all_dates = [d for d in all_dates if len(d) >= 8]  # 确保有足够的日期信息
                    if all_dates:
                        date_coverage[dt] = {
                            'min_date': min(all_dates),
                            'max_date': max(all_dates),
                            'unique_count': len(set(all_dates))
                        }
            
            else:
                # 非分区存储
                if storage_path.exists():
                    df = pl.read_parquet(storage_path)
                    date_field = config['storage']['partition_field']
                    
                    if date_field and date_field in df.columns:
                        dates = df[date_field].drop_nulls().cast(pl.Utf8).to_list()
                        dates = [d.replace('-', '').replace('/', '') for d in dates if d]
                        dates = [d for d in dates if len(d) >= 8]  # 确保有足够的日期信息
                        
                        if dates:
                            date_coverage[dt] = {
                                'min_date': min(dates),
                                'max_date': max(dates),
                                'unique_count': len(set(dates))
                            }
        
        except Exception:
            continue
    
    # 打印时间覆盖范围
    for dt, coverage in date_coverage.items():
        print(f"{dt:20} | {coverage['min_date']} ~ {coverage['max_date']} | {coverage['unique_count']} 天")


def main():
    setup_logging()
    
    print("数据完整性检查工具")
    print("支持的命令:")
    print("  python data_integrity_check.py                    # 检查所有数据类型")
    print("  python data_integrity_check.py <data_type>        # 检查指定数据类型")
    
    import sys
    if len(sys.argv) > 1:
        data_type = sys.argv[1]
        check_data_integrity(data_type)
    else:
        check_data_integrity()
        check_data_coverage_by_date_range()


if __name__ == "__main__":
    main()