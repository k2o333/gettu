# validate_storage_config.py
from pathlib import Path

def validate_storage_configuration():
    """验证存储配置与实际存储的一致性"""
    from config import DATA_INTERFACE_CONFIG, PartitionGranularity

    inconsistencies = []
    validations = []  # 记录所有验证结果

    for data_type, config in DATA_INTERFACE_CONFIG.items():
        storage_path = config['storage']['path']
        expected_granularity = config['storage']['partition_granularity']

        validation_result = {
            'data_type': data_type,
            'expected_granularity': expected_granularity,
            'path': str(storage_path),
            'is_valid': False
        }

        if expected_granularity != PartitionGranularity.NONE:
            # 检查是否按分区格式存储
            if expected_granularity == 'year':
                # 检查是否有年份子目录
                if storage_path.exists():
                    has_year_partitions = any((storage_path / d).is_dir() for d in storage_path.iterdir() if d.is_dir() and d.name.startswith('year='))
                    if has_year_partitions:
                        validation_result['is_valid'] = True
                        validation_result['actual'] = 'year partition'
                    else:
                        # 检查是否为非分区文件
                        non_partition_file = Path(f"{storage_path}.parquet") if str(storage_path).endswith('.parquet') == False else storage_path
                        if non_partition_file.exists():
                            validation_result['actual'] = 'non-partitioned file'
                        else:
                            validation_result['actual'] = 'missing'
                        inconsistencies.append({
                            'data_type': data_type,
                            'expected': 'year partition',
                            'actual': validation_result['actual'],
                            'path': str(storage_path)
                        })
                else:
                    # 路径不存在
                    validation_result['actual'] = 'missing'
                    inconsistencies.append({
                        'data_type': data_type,
                        'expected': 'year partition',
                        'actual': 'missing',
                        'path': str(storage_path)
                    })
            elif expected_granularity == 'year_month':
                # 检查是否有年月子目录
                if storage_path.exists():
                    has_month_partitions = any((storage_path / d).is_dir() for d in storage_path.iterdir() if d.is_dir() and d.name.startswith('year_month='))
                    if has_month_partitions:
                        validation_result['is_valid'] = True
                        validation_result['actual'] = 'year_month partition'
                    else:
                        # 检查是否为非分区文件
                        non_partition_file = Path(f"{storage_path}.parquet") if str(storage_path).endswith('.parquet') == False else storage_path
                        if non_partition_file.exists():
                            validation_result['actual'] = 'non-partitioned file'
                        else:
                            validation_result['actual'] = 'missing'
                        inconsistencies.append({
                            'data_type': data_type,
                            'expected': 'year_month partition',
                            'actual': validation_result['actual'],
                            'path': str(storage_path)
                        })
                else:
                    # 路径不存在
                    validation_result['actual'] = 'missing'
                    inconsistencies.append({
                        'data_type': data_type,
                        'expected': 'year_month partition',
                        'actual': 'missing',
                        'path': str(storage_path)
                    })
        else:
            # 非分区存储，检查文件是否存在
            non_partition_path = Path(f"{storage_path}.parquet") if str(storage_path).endswith('.parquet') == False else storage_path
            if non_partition_path.exists():
                validation_result['is_valid'] = True
                validation_result['actual'] = 'non-partitioned file'
            else:
                validation_result['actual'] = 'missing'
                validation_result['is_valid'] = False
                inconsistencies.append({
                    'data_type': data_type,
                    'expected': 'non-partitioned file',
                    'actual': 'missing',
                    'path': str(non_partition_path)
                })

        validations.append(validation_result)

    # 输出详细验证报告
    print(f"存储配置验证报告: {len(validations)} 个数据类型中，{len(inconsistencies)} 个存在问题")
    print("="*80)

    for result in validations:
        status = "✅" if result['is_valid'] else "❌"
        actual_value = result.get('actual', 'unknown')
        print(f"{status} {result['data_type']}: 期望 {result['expected_granularity']}, 实际 {actual_value}")

    if inconsistencies:
        print("\n问题概要:")
        for item in inconsistencies:
            print(f"  {item['data_type']}: 期望 {item['expected']}, 实际 {item['actual']} - {item['path']}")
    else:
        print("\n所有数据存储配置与实际存储一致")

    return inconsistencies, validations