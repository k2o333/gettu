import polars as pl
import logging
import shutil
from pathlib import Path
import tempfile
import os
from config import COMPRESSION_TYPE, CHUNK_SIZE

def atomic_partitioned_sink(lazy_frame, base_path: Path, partition_by: list):
    """
    原子性分区写入
    将数据按分区写入，避免数据损坏
    """
    try:
        # 确保基础路径存在
        base_path.mkdir(parents=True, exist_ok=True)

        # 收集数据
        df = lazy_frame.collect()

        # 检查分区字段是否存在
        missing_cols = [col for col in partition_by if col not in df.columns]
        if missing_cols:
            logging.warning(f"分区字段不存在: {missing_cols}")
            return

        # 按分区字段分组
        for partition_values in df.select(partition_by).unique().iter_rows():
            # 构建分区条件
            condition = True
            partition_name_parts = []
            for i, col in enumerate(partition_by):
                val = partition_values[i]
                condition = condition & (pl.col(col) == val)
                partition_name_parts.append(f"{col}={val}")

            # 筛选当前分区数据
            partition_df = df.filter(condition)

            if len(partition_df) == 0:
                continue

            # 构建分区目录
            partition_dir = base_path
            for i, col in enumerate(partition_by):
                val = partition_values[i]
                partition_dir = partition_dir / f"{col}={val}"

            # 确保分区目录存在
            partition_dir.mkdir(parents=True, exist_ok=True)

            # 定义输出文件路径
            output_path = partition_dir / "data.parquet"

            # 使用临时文件进行原子写入
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                try:
                    # 写入临时文件
                    partition_df.write_parquet(
                        tmp_file.name,
                        compression=COMPRESSION_TYPE,
                        row_group_size=CHUNK_SIZE
                    )

                    # 原子性替换
                    if output_path.exists():
                        # 先备份原文件
                        backup_path = output_path.with_suffix(output_path.suffix + '.backup')
                        if backup_path.exists():
                            backup_path.unlink()
                        output_path.rename(backup_path)

                    # 将临时文件移动到目标位置
                    Path(tmp_file.name).rename(output_path)

                    # 删除备份文件
                    if 'backup_path' in locals() and backup_path.exists():
                        backup_path.unlink()

                    logging.info(f"成功写入分区 {partition_dir.name}: {len(partition_df)} 条记录")
                except Exception as e:
                    # 如果出错，删除临时文件
                    if Path(tmp_file.name).exists():
                        Path(tmp_file.name).unlink()
                    logging.error(f"写入分区 {partition_dir.name} 失败: {str(e)}")
                    raise

    except Exception as e:
        logging.error(f"atomic_partitioned_sink 失败: {str(e)}")
        raise


def atomic_unpartitioned_sink(lazy_frame, output_path: str):
    """
    原子性非分区写入
    将数据写入单个文件，避免数据损坏
    """
    try:
        # 确保输出路径的目录存在
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # 使用临时文件进行原子写入
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            try:
                # 收集并写入临时文件
                lazy_frame.collect().write_parquet(
                    tmp_file.name,
                    compression=COMPRESSION_TYPE,
                    row_group_size=CHUNK_SIZE
                )

                # 原子性替换
                tmp_path = Path(tmp_file.name)
                if Path(output_path).exists():
                    # 先备份原文件
                    backup_path = Path(output_path).with_suffix(Path(output_path).suffix + '.backup')
                    if backup_path.exists():
                        backup_path.unlink()
                    Path(output_path).rename(backup_path)

                # 将临时文件移动到目标位置
                tmp_path.rename(output_path)

                # 删除备份文件
                if 'backup_path' in locals() and backup_path.exists():
                    backup_path.unlink()

                logging.info(f"成功写入文件 {output_path}")
            except Exception as e:
                # 如果出错，删除临时文件
                if tmp_path.exists():
                    tmp_path.unlink()
                logging.error(f"写入文件 {output_path} 失败: {str(e)}")
                raise

    except Exception as e:
        logging.error(f"atomic_unpartitioned_sink 失败: {str(e)}")
        raise


def check_storage_health(base_path: Path, partition_by: list = None):
    """
    检查存储健康状况
    """
    try:
        if not base_path.exists():
            logging.warning(f"存储路径不存在: {base_path}")
            return False

        if partition_by:
            # 检查分区存储
            for partition_dir in base_path.iterdir():
                if partition_dir.is_dir():
                    data_file = partition_dir / "data.parquet"
                    if not data_file.exists():
                        logging.warning(f"分区数据文件不存在: {data_file}")
                        continue

                    # 尝试读取文件以检查完整性
                    try:
                        df = pl.read_parquet(data_file)
                        logging.info(f"分区 {partition_dir.name} 健康: {len(df)} 条记录")
                    except Exception as e:
                        logging.error(f"分区 {partition_dir.name} 损坏: {str(e)}")
                        return False
        else:
            # 检查非分区存储
            if not Path(base_path).exists():
                logging.warning(f"数据文件不存在: {base_path}")
                return False

            try:
                df = pl.read_parquet(base_path)
                logging.info(f"数据文件健康: {len(df)} 条记录")
            except Exception as e:
                logging.error(f"数据文件损坏: {str(e)}")
                return False

        return True
    except Exception as e:
        logging.error(f"检查存储健康状况失败: {str(e)}")
        return False


def optimize_storage(base_path: Path, partition_by: list = None):
    """
    优化存储 - 合并小文件、压缩等
    """
    try:
        logging.info(f"开始优化存储: {base_path}")
        
        # 对于分区存储，可以考虑合并小分区文件
        if partition_by and base_path.exists():
            for partition_dir in base_path.iterdir():
                if partition_dir.is_dir():
                    data_file = partition_dir / "data.parquet"
                    if data_file.exists():
                        # 检查文件大小，如果太小则可能需要合并（在实际系统中实现）
                        file_size = data_file.stat().st_size
                        if file_size < 10 * 1024 * 1024:  # 小于10MB
                            logging.info(f"小文件检测: {data_file} ({file_size} bytes)")
        
        logging.info(f"存储优化完成: {base_path}")
    except Exception as e:
        logging.error(f"优化存储失败: {str(e)}")
        raise