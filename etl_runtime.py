import polars as pl
import logging
import psutil
from config import DATA_INTERFACE_CONFIG, FIELD_MAPPING_CONFIG, FIELD_TYPE_CONFIG, PartitionGranularity, get_dynamic_streaming_threshold
from storage import atomic_partitioned_sink, atomic_unpartitioned_sink
from memory_monitor import memory_monitor, memory_safe_operation

# Import data validation components
try:
    from data_validation import DataValidationOrchestrator
    DATA_VALIDATION_AVAILABLE = True
except ImportError:
    DATA_VALIDATION_AVAILABLE = False
    logging.warning("Data validation components not available. Skipping data quality checks.")

# Import enhanced partition functionality
try:
    from storage import (
        atomic_partitioned_sink, atomic_unpartitioned_sink,
        enhanced_yearly_partitioned_sink, enhanced_monthly_partitioned_sink,
        optimize_partition_storage, adjust_partition_strategy,
        manage_partition_metadata, manage_partition_lifecycle,
        monitor_partition_performance, analyze_partition_query_performance
    )
    PARTITION_ENHANCEMENT_AVAILABLE = True
except ImportError:
    PARTITION_ENHANCEMENT_AVAILABLE = False
    logging.warning("Enhanced partition functionality not available. Using basic partitioning.")

class EtlRuntime:
    """配置驱动ETL处理器"""
    
    @staticmethod
    def process_data(data_type: str, df=None, source_path=None, validate_data: bool = True):
        """根据配置执行ETL处理"""
        # 记录开始时的内存使用情况
        start_memory = psutil.virtual_memory().percent
        logging.info(f"开始处理 {data_type}，初始内存使用率: {start_memory}%")

        if data_type not in DATA_INTERFACE_CONFIG:
            raise ValueError(f"不支持的数据类型: {data_type}")

        config = DATA_INTERFACE_CONFIG[data_type]

        # 1. 加载数据
        if df is not None:
            lf = df.lazy()
        elif source_path is not None:
            lf = pl.scan_parquet(source_path)
        else:
            raise ValueError("必须提供DataFrame或source_path")

        # 2. 数据验证（如果启用）
        # 在需要验证时收集数据，否则保持懒加载
        if validate_data and DATA_VALIDATION_AVAILABLE:
            try:
                # 只在需要验证时才收集数据
                df_for_validation = lf.collect()
                EtlRuntime._validate_data(df_for_validation, data_type)
                # 验证后重新设置为懒加载
                lf = df_for_validation.lazy()
            except Exception as e:
                logging.warning(f"数据验证过程中出现错误: {str(e)}")
                # Continue with ETL process even if validation fails

        # 3. 字段标准化（根据FIELD_MAPPING_CONFIG）
        lf = EtlRuntime._normalize_fields(lf, config)

        # 4. ID增强（根据配置决定是否需要ts_code）
        if 'ts_code' in lf.collect_schema().names():
            # Use dictionary management system for ID conversion
            from dictionary_management import DictionaryManager
            dict_manager = DictionaryManager()
            dict_manager.initialize()

            # Convert ts_code to ts_code_id using dictionary management system
            def map_ts_code_to_id(ts_code):
                if ts_code is None:
                    return None
                return dict_manager.get_stock_id(ts_code)

            lf = lf.with_columns([
                pl.col('ts_code').map_elements(
                    map_ts_code_to_id,
                    return_dtype=pl.Int64
                ).alias('ts_code_id')
            ]).drop('ts_code')

        # 5. 数据类型优化（根据FIELD_TYPE_CONFIG）
        lf = EtlRuntime._type_optimization(lf, config)

        # 6. 排序（根据配置的sort_fields）
        sort_fields = config['storage']['sort_fields']
        available_fields = [f for f in sort_fields if f in lf.collect_schema().names()]
        if available_fields:
            lf = lf.sort(available_fields)

        # 7. 存储（根据配置的分区策略）
        EtlRuntime._write_data(lf, config, data_type)

        # 记录结束时的内存使用情况
        end_memory = psutil.virtual_memory().percent
        logging.info(f"完成处理 {data_type}，最终内存使用率: {end_memory}%，变化: {end_memory - start_memory}%")

    @staticmethod
    def _validate_data(df: pl.DataFrame, data_type: str):
        """对数据进行质量验证"""
        if not DATA_VALIDATION_AVAILABLE:
            return

        logging.info(f"开始对数据类型 {data_type} 进行数据质量验证")

        try:
            # Initialize the data validation orchestrator
            validator = DataValidationOrchestrator()

            # Perform validation based on data type
            validation_results = validator.validate_dataset(
                df,
                dataset_type=data_type,
                dataset_name=data_type
            )

            # Log validation results
            if validation_results.get('overall_valid', True):
                logging.info(f"数据类型 {data_type} 通过质量验证")
            else:
                logging.warning(f"数据类型 {data_type} 存在质量问题")

                # Log specific issues
                issues = validation_results.get('issues', [])
                for issue in issues:
                    logging.warning(f"验证问题 - {issue.get('description', 'Unknown issue')}")

                # Check for anomalies
                anomaly_info = validation_results.get('anomaly_detection', {})
                anomaly_count = anomaly_info.get('total_anomalies', 0)
                if anomaly_count > 0:
                    logging.warning(f"检测到 {anomaly_count} 个价格异常")

                # Check for consistency issues
                consistency_results = validation_results.get('logical_consistency', {})
                consistency_issues = consistency_results.get('issues', [])
                if consistency_issues:
                    logging.warning(f"检测到 {len(consistency_issues)} 个逻辑一致性问题")

        except Exception as e:
            logging.error(f"数据验证过程中出现错误: {str(e)}")
            raise

    @staticmethod
    def _normalize_fields(lazy_frame, config):
        """根据配置标准化字段"""
        from datetime import datetime

        # 调试信息：打印原始列名
        original_columns = lazy_frame.collect_schema().names()
        logging.debug(f"原始列名: {original_columns}")

        # 根据FIELD_MAPPING_CONFIG映射字段名
        # 首先收集所有现有的列
        current_columns = set(lazy_frame.collect_schema().names())
        processed_columns = set()  # 已处理的原始列名，避免重复处理

        # 为避免字段名冲突，我们按配置的顺序处理，并优先处理更具体的字段名
        # 按照字段重要性排序：trade_date是高频使用的，应优先处理
        field_priority_order = ['trade_date', 'ann_date', 'ts_code']  # 按重要性排序

        # 按优先级处理字段映射
        for standardized_name in field_priority_order:
            if standardized_name in FIELD_MAPPING_CONFIG:
                possible_names = FIELD_MAPPING_CONFIG[standardized_name]
                # 按可能名称的顺序处理（通常最具体的名称在前）
                for original_name in possible_names:
                    # 检查原始字段是否存在于当前列中，且不是标准化名称（避免重命名标准化名称）
                    # 且尚未被处理过
                    if original_name in current_columns and original_name != standardized_name and original_name not in processed_columns:
                        lazy_frame = lazy_frame.rename({original_name: standardized_name})
                        processed_columns.add(original_name)  # 标记为已处理
                        break  # 一旦找到并重命名，就跳出，避免重复映射

        # 处理剩余未按优先级的配置
        for standardized_name, possible_names in FIELD_MAPPING_CONFIG.items():
            if standardized_name not in field_priority_order:  # 只处理不在优先级列表中的字段
                for original_name in possible_names:
                    if original_name in current_columns and original_name != standardized_name and original_name not in processed_columns:
                        lazy_frame = lazy_frame.rename({original_name: standardized_name})
                        processed_columns.add(original_name)

        # 添加分区字段（根据partition_field配置）
        partition_field = config['storage']['partition_field']
        available_columns = lazy_frame.collect_schema().names()
        logging.debug(f"处理后列名: {available_columns}")
        logging.debug(f"分区字段配置: {partition_field}")
        logging.debug(f"分区字段是否存在: {partition_field in available_columns if partition_field else 'N/A'}")

        if partition_field and partition_field in available_columns:
            # 创建整数格式的日期字段
            int_field = f"{partition_field}_int"
            logging.debug(f"创建整数日期字段: {int_field} 从 {partition_field}")
            lazy_frame = lazy_frame.with_columns([
                pl.col(partition_field).cast(pl.Utf8).str.replace('-', '').str.replace('/', '').cast(pl.UInt32, strict=False).alias(int_field)
            ])

            # 根据分区粒度添加分区字段
            granularity = config['storage']['partition_granularity']
            logging.debug(f"分区粒度: {granularity}")
            if granularity == 'year' or granularity == PartitionGranularity.YEAR:
                logging.debug("创建year分区字段")
                lazy_frame = lazy_frame.with_columns([
                    (pl.col(int_field) // 10000).cast(pl.UInt32).alias('year')
                ])
            elif granularity == 'year_month' or granularity == PartitionGranularity.YEAR_MONTH:
                logging.debug("创建year_month分区字段")
                lazy_frame = lazy_frame.with_columns([
                    (pl.col(int_field) // 100).cast(pl.UInt32).alias('year_month')
                ])
        else:
            # 检查是否是不需要分区字段的情况（如非分区存储）
            partition_granularity = config['storage']['partition_granularity']
            logging.debug(f"分区粒度配置: {partition_granularity}")
            if partition_granularity != PartitionGranularity.NONE and partition_granularity != 'none':
                # 如果配置要求分区但分区字段不存在，则中断执行
                logging.error(f"分区字段 {partition_field} 不存在，立即中断执行。配置要求分区存储但数据中不包含必需的分区字段。")
                logging.error(f"当前可用列: {available_columns}")
                raise ValueError(f"无法找到配置要求的分区字段: {partition_field}")

        return lazy_frame
    
    @staticmethod
    def _type_optimization(lazy_frame, config):
        """优化数据类型"""
        # 根据FIELD_TYPE_CONFIG进行数据类型优化
        for field_name in lazy_frame.columns:
            if field_name in FIELD_TYPE_CONFIG['date_fields']:
                # 日期字段通常已经是字符串，这里可以根据需要进行转换
                pass
            elif field_name in FIELD_TYPE_CONFIG['id_fields']:
                # ID字段使用合适的类型
                if lazy_frame.schema[field_name] == pl.Utf8:
                    # 可以将ID字段转换为类别类型以节省内存
                    pass
            elif field_name in FIELD_TYPE_CONFIG['numeric_fields']:
                # 确保数值字段为数值类型
                if lazy_frame.schema[field_name] == pl.Utf8:
                    # 尝试转换为浮点数
                    lazy_frame = lazy_frame.with_columns([
                        pl.col(field_name).str.replace(',', '').cast(pl.Float64, strict=False).alias(field_name)
                    ])
        
        return lazy_frame
    
    @staticmethod
    def _write_data(lazy_frame, config, data_type):
        """根据配置写入数据"""
        base_path = config['storage']['path']
        partition_granularity = config['storage']['partition_granularity']

        if PARTITION_ENHANCEMENT_AVAILABLE:
            # 使用增强的分区功能
            if partition_granularity == PartitionGranularity.YEAR or partition_granularity == 'year':
                # 检查 year 字段是否存在
                available_columns = lazy_frame.collect_schema().names()
                logging.debug(f"写入前检查列名: {available_columns}")
                if 'year' not in lazy_frame.collect_schema().names():
                    logging.error(f"{data_type}: 无法找到 'year' 分区字段，立即中断执行。请检查 {config['storage']['partition_field']} 字段是否存在。")
                    logging.error(f"当前可用列: {available_columns}")
                    raise ValueError(f"配置为年分区存储但分区字段 'year' 不存在: {data_type}")
                else:
                    # 记录分区字段的唯一值以供监控
                    unique_years = lazy_frame.select(pl.col('year').unique()).collect()
                    logging.debug(f"{data_type}: 发现分区年份 {unique_years['year'].to_list()}")
                    # 使用增强的年分区功能
                    enhanced_yearly_partitioned_sink(lazy_frame, base_path, data_type=data_type)
                    # 优化分区存储
                    optimize_partition_storage(base_path, optimization_strategy="auto")
            elif partition_granularity == PartitionGranularity.YEAR_MONTH or partition_granularity == 'year_month':
                # 检查 year_month 字段是否存在
                if 'year_month' not in lazy_frame.collect_schema().names():
                    logging.error(f"{data_type}: 无法找到 'year_month' 分区字段，立即中断执行。请检查 {config['storage']['partition_field']} 字段是否存在。")
                    raise ValueError(f"配置为年月分区存储但分区字段 'year_month' 不存在: {data_type}")
                else:
                    # 记录分区字段的唯一值以供监控
                    unique_months = lazy_frame.select(pl.col('year_month').unique()).collect()
                    logging.debug(f"{data_type}: 发现分区年月 {unique_months['year_month'].to_list()}")
                    # 使用增强的月分区功能
                    enhanced_monthly_partitioned_sink(lazy_frame, base_path, data_type=data_type)
                    # 优化分区存储
                    optimize_partition_storage(base_path, optimization_strategy="auto")
            else:
                # 非分区存储
                output_path = f"{base_path}.parquet" if not str(base_path).endswith('.parquet') else str(base_path)
                atomic_unpartitioned_sink(lazy_frame, output_path, data_type=data_type)

            # 动态调整分区策略
            try:
                if partition_granularity != PartitionGranularity.NONE and partition_granularity != 'none':
                    adjust_partition_strategy(base_path, data_type=data_type)
            except Exception as e:
                logging.warning(f"动态调整分区策略失败: {str(e)}")
        else:
            # 使用基本分区功能
            if partition_granularity == PartitionGranularity.YEAR or partition_granularity == 'year':
                # 检查 year 字段是否存在
                available_columns = lazy_frame.collect_schema().names()
                logging.debug(f"基本分区写入前检查列名: {available_columns}")
                if 'year' not in lazy_frame.collect_schema().names():
                    logging.error(f"{data_type}: 无法找到 'year' 分区字段，立即中断执行。请检查 {config['storage']['partition_field']} 字段是否存在。")
                    logging.error(f"当前可用列: {available_columns}")
                    raise ValueError(f"配置为年分区存储但分区字段 'year' 不存在: {data_type}")
                else:
                    # 记录分区字段的唯一值以供监控
                    unique_years = lazy_frame.select(pl.col('year').unique()).collect()
                    logging.debug(f"{data_type}: 发现分区年份 {unique_years['year'].to_list()}")
                    atomic_partitioned_sink(lazy_frame, base_path, partition_by=['year'])
            elif partition_granularity == PartitionGranularity.YEAR_MONTH or partition_granularity == 'year_month':
                # 检查 year_month 字段是否存在
                if 'year_month' not in lazy_frame.collect_schema().names():
                    logging.error(f"{data_type}: 无法找到 'year_month' 分区字段，立即中断执行。请检查 {config['storage']['partition_field']} 字段是否存在。")
                    raise ValueError(f"配置为年月分区存储但分区字段 'year_month' 不存在: {data_type}")
                else:
                    # 记录分区字段的唯一值以供监控
                    unique_months = lazy_frame.select(pl.col('year_month').unique()).collect()
                    logging.debug(f"{data_type}: 发现分区年月 {unique_months['year_month'].to_list()}")
                    atomic_partitioned_sink(lazy_frame, base_path, partition_by=['year_month'])
            else:
                # 非分区存储
                output_path = f"{base_path}.parquet" if not str(base_path).endswith('.parquet') else str(base_path)
                atomic_unpartitioned_sink(lazy_frame, output_path)

        logging.info(f"ETL完成: {data_type} -> {base_path}")


class FieldTransformer:
    """字段转换器，用于标准化字段名"""
    
    @staticmethod
    def standardize_field_names(df, data_type):
        """标准化字段名"""
        if data_type not in DATA_INTERFACE_CONFIG:
            return df
            
        config = DATA_INTERFACE_CONFIG[data_type]
        
        # 根据FIELD_MAPPING_CONFIG映射字段名
        for standardized_name, possible_names in FIELD_MAPPING_CONFIG.items():
            for original_name in possible_names:
                if original_name in df.columns and original_name != standardized_name:
                    df = df.rename({original_name: standardized_name})
        
        return df