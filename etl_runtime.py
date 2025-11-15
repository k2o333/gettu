import polars as pl
import logging
from config import DATA_INTERFACE_CONFIG, FIELD_MAPPING_CONFIG, FIELD_TYPE_CONFIG, PartitionGranularity
from storage import atomic_partitioned_sink, atomic_unpartitioned_sink

class EtlRuntime:
    """配置驱动ETL处理器"""
    
    @staticmethod
    def process_data(data_type: str, df=None, source_path=None):
        """根据配置执行ETL处理"""
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

        # 2. 字段标准化（根据FIELD_MAPPING_CONFIG）
        lf = EtlRuntime._normalize_fields(lf, config)

        # 3. ID增强（根据配置决定是否需要ts_code）
        if 'ts_code' in lf.columns:
            from dictionaries import get_ts_code_dict_path
            ts_code_dict_path = get_ts_code_dict_path()
            if ts_code_dict_path.exists():
                lf = lf.join(
                    pl.scan_parquet(ts_code_dict_path),
                    on='ts_code',
                    how='left'
                ).drop('ts_code')

        # 4. 数据类型优化（根据FIELD_TYPE_CONFIG）
        lf = EtlRuntime._type_optimization(lf, config)

        # 5. 排序（根据配置的sort_fields）
        sort_fields = config['storage']['sort_fields']
        available_fields = [f for f in sort_fields if f in lf.collect_schema().names()]
        if available_fields:
            lf = lf.sort(available_fields)

        # 6. 存储（根据配置的分区策略）
        EtlRuntime._write_data(lf, config, data_type)

        # 触发执行
        if df is not None:
            lf.collect()
    
    @staticmethod
    def _normalize_fields(lazy_frame, config):
        """根据配置标准化字段"""
        from datetime import datetime

        # 根据FIELD_MAPPING_CONFIG映射字段名
        for standardized_name, possible_names in FIELD_MAPPING_CONFIG.items():
            for original_name in possible_names:
                if original_name in lazy_frame.columns and original_name != standardized_name:
                    lazy_frame = lazy_frame.rename({original_name: standardized_name})

        # 添加分区字段（根据partition_field配置）
        partition_field = config['storage']['partition_field']
        if partition_field and partition_field in lazy_frame.columns:
            # 创建整数格式的日期字段
            int_field = f"{partition_field}_int"
            lazy_frame = lazy_frame.with_columns([
                pl.col(partition_field).cast(pl.Utf8).str.replace('-', '').str.replace('/', '').cast(pl.UInt32, strict=False).alias(int_field)
            ])

            # 验证转换结果，如果转换失败，立即中断执行
            try:
                # 根据分区粒度添加分区字段
                granularity = config['storage']['partition_granularity']
                if granularity == 'year':
                    lazy_frame = lazy_frame.with_columns([
                        (pl.col(int_field) // 10000).cast(pl.UInt32).alias('year')
                    ])
                elif granularity == 'year_month':
                    lazy_frame = lazy_frame.with_columns([
                        (pl.col(int_field) // 100).cast(pl.UInt32).alias('year_month')
                    ])
            except Exception as e:
                logging.error(f"无法从 {partition_field} 生成分区字段: {str(e)}，立即中断执行。请确保数据中存在有效的日期字段以支持配置的分区策略。")
                raise RuntimeError(f"分区字段生成失败: {e}")
        else:
            # 如果分区字段不存在，立即中断执行而不是降级
            logging.error(f"分区字段 {partition_field} 不存在，立即中断执行。配置要求分区存储但数据中不包含必需的分区字段。")
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

        if partition_granularity == PartitionGranularity.YEAR or partition_granularity == 'year':
            # 检查 year 字段是否存在
            if 'year' not in lazy_frame.columns:
                logging.error(f"{data_type}: 无法找到 'year' 分区字段，立即中断执行。请检查 {config['storage']['partition_field']} 字段是否存在。")
                raise ValueError(f"配置为年分区存储但分区字段 'year' 不存在: {data_type}")
            else:
                # 记录分区字段的唯一值以供监控
                unique_years = lazy_frame.select(pl.col('year').unique()).collect()
                logging.debug(f"{data_type}: 发现分区年份 {unique_years['year'].to_list()}")
                atomic_partitioned_sink(lazy_frame, base_path, partition_by=['year'])
        elif partition_granularity == PartitionGranularity.YEAR_MONTH or partition_granularity == 'year_month':
            # 检查 year_month 字段是否存在
            if 'year_month' not in lazy_frame.columns:
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