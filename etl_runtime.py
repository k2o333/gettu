import polars as pl
import logging
from config import DATA_INTERFACE_CONFIG, FIELD_MAPPING_CONFIG, FIELD_TYPE_CONFIG
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
    
    @staticmethod
    def _normalize_fields(lazy_frame, config):
        """根据配置标准化字段"""
        # 根据FIELD_MAPPING_CONFIG映射字段名
        for standardized_name, possible_names in FIELD_MAPPING_CONFIG.items():
            for original_name in possible_names:
                if original_name in lazy_frame.columns and original_name != standardized_name:
                    lazy_frame = lazy_frame.rename({original_name: standardized_name})

        # 添加分区字段（根据partition_field配置）
        partition_field = config['storage']['partition_field']
        if partition_field in lazy_frame.columns:
            int_field = f"{partition_field}_int"
            lazy_frame = lazy_frame.with_columns([
                pl.col(partition_field).cast(pl.Utf8).str.replace('-', '').cast(pl.UInt32).alias(int_field)
            ])

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

        if partition_granularity == 'year':
            atomic_partitioned_sink(lazy_frame, base_path, partition_by=['year'])
        elif partition_granularity == 'year_month':
            atomic_partitioned_sink(lazy_frame, base_path, partition_by=['year_month'])
        else:
            # 确保输出是.parquet文件
            output_path = f"{base_path}.parquet" if not str(base_path).endswith('.parquet') else base_path
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