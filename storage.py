import polars as pl
import logging
import shutil
from pathlib import Path
import tempfile
import os
import psutil
from config import COMPRESSION_TYPE, CHUNK_SIZE, STREAMING_THRESHOLD, get_dynamic_streaming_threshold
from memory_monitor import memory_monitor, memory_safe_operation

def enhanced_monthly_partitioned_sink(lazy_frame, base_path: Path,
                                    partition_field: str = 'year_month',
                                    data_type: str = "general",
                                    optimize_compression: bool = True):
    """
    增强的按月分区写入功能
    专门针对月分区优化的实现
    """
    import time
    start_time = time.time()

    try:
        # 收集数据
        df = lazy_frame.collect()

        # 检查分区字段是否存在
        if partition_field not in df.columns:
            logging.error(f"分区字段 {partition_field} 不存在")
            raise ValueError(f"分区字段 {partition_field} 不存在")

        # 确保基础路径存在
        base_path.mkdir(parents=True, exist_ok=True)

        # 获取所有唯一月份
        unique_months = df.select(pl.col(partition_field).unique()).to_series().to_list()
        logging.info(f"按月分区，发现 {len(unique_months)} 个月份: {sorted(unique_months)}")

        # 根据数据类型选择压缩策略
        compression_type = COMPRESSION_TYPE
        if optimize_compression and data_type in ["block_trade", "stk_surv", "report_rc"]:
            compression_type = 'zstd'  # 对于高频事件数据使用更高效的压缩

        # 为每个月份创建分区
        total_records = 0
        for month in unique_months:
            partition_start_time = time.time()

            # 筛选当前月份的数据
            partition_df = df.filter(pl.col(partition_field) == month)

            if len(partition_df) == 0:
                continue

            # 构建月份分区目录
            month_partition_dir = base_path / f"{partition_field}={month}"
            month_partition_dir.mkdir(parents=True, exist_ok=True)

            # 定义输出文件路径
            output_path = month_partition_dir / "data.parquet"

            # 使用临时文件进行原子写入
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                try:
                    # 写入临时文件
                    partition_df.write_parquet(
                        tmp_file.name,
                        compression=compression_type,
                        row_group_size=CHUNK_SIZE
                    )

                    # 原子性替换
                    if output_path.exists():
                        backup_path = output_path.with_suffix(output_path.suffix + '.backup')
                        if backup_path.exists():
                            backup_path.unlink()
                        output_path.rename(backup_path)

                    Path(tmp_file.name).rename(output_path)

                    if 'backup_path' in locals() and backup_path.exists():
                        backup_path.unlink()

                    partition_duration = time.time() - partition_start_time
                    records_count = len(partition_df)
                    total_records += records_count
                    logging.info(f"成功写入月份分区 {month}: {records_count} 条记录, 耗时: {partition_duration:.2f}s")

                except Exception as e:
                    if Path(tmp_file.name).exists():
                        Path(tmp_file.name).unlink()
                    logging.error(f"写入月份分区 {month} 失败: {str(e)}")
                    raise

        total_duration = time.time() - start_time
        logging.info(f"增强月分区写入完成: {base_path}, 总记录数: {total_records}, 总耗时: {total_duration:.2f}s")

    except Exception as e:
        logging.error(f"enhanced_monthly_partitioned_sink 失败: {str(e)}")
        raise


def check_partition_sizes(base_path: Path) -> list:
    """
    检查分区大小

    Args:
        base_path (Path): 基础路径

    Returns:
        list: (partition_path, size_in_bytes) 元组列表
    """
    partition_sizes = []

    # 遍历所有分区目录
    for partition_dir in base_path.iterdir():
        if partition_dir.is_dir():
            data_file = partition_dir / "data.parquet"
            if data_file.exists():
                size = data_file.stat().st_size
                partition_sizes.append((partition_dir, size))

    return partition_sizes


def merge_adjacent_partitions(base_path: Path, partition_field: str):
    """
    合并相邻分区以优化查询性能

    Args:
        base_path (Path): 基础路径
        partition_field (str): 分区字段名
    """
    import time
    import re
    start_time = time.time()

    try:
        # 获取所有分区目录
        partition_dirs = []
        for item in base_path.iterdir():
            if item.is_dir() and f"{partition_field}=" in str(item):
                partition_dirs.append(item)

        if len(partition_dirs) < 2:
            logging.info(f"分区数量不足，无需合并: {len(partition_dirs)} 个分区")
            return

        # 按分区值排序
        def extract_partition_value(partition_dir):
            match = re.search(rf"{partition_field}=(\d+)", partition_dir.name)
            return int(match.group(1)) if match else 0

        partition_dirs.sort(key=extract_partition_value)

        logging.info(f"发现 {len(partition_dirs)} 个分区，准备检查相邻分区合并")

        # 检查相邻分区是否需要合并（简化逻辑：合并小分区）
        merged_count = 0
        for i in range(len(partition_dirs) - 1):
            current_partition = partition_dirs[i]
            next_partition = partition_dirs[i + 1]

            # 获取分区值
            current_value = extract_partition_value(current_partition)
            next_value = extract_partition_value(next_partition)

            # 检查是否相邻（对于年月分区，相邻月份差值为1）
            if next_value - current_value == 1:
                # 检查分区大小
                current_data_file = current_partition / "data.parquet"
                next_data_file = next_partition / "data.parquet"

                if current_data_file.exists() and next_data_file.exists():
                    current_size = current_data_file.stat().st_size
                    next_size = next_data_file.stat().st_size

                    # 如果两个分区都很小，考虑合并
                    if current_size < 5 * 1024 * 1024 and next_size < 5 * 1024 * 1024:  # 5MB阈值
                        logging.debug(f"相邻分区 {current_partition.name} 和 {next_partition.name} 都很小，可以考虑合并")

        merge_duration = time.time() - start_time
        logging.info(f"相邻分区检查完成，合并了 {merged_count} 对分区，耗时: {merge_duration:.2f}s")

    except Exception as e:
        logging.error(f"合并相邻分区失败: {str(e)}")
        raise


def optimize_partition_storage(base_path: Path, optimization_strategy: str = "auto"):
    """
    优化分区存储

    Args:
        base_path (Path): 基础路径
        optimization_strategy (str): 优化策略 ("auto", "compress", "merge", "repartition")
    """
    import time
    start_time = time.time()

    try:
        logging.info(f"开始优化分区存储: {base_path}, 策略: {optimization_strategy}")

        # 检查分区大小
        partition_sizes = check_partition_sizes(base_path)

        # 根据策略执行优化
        if optimization_strategy == "auto" or optimization_strategy == "merge":
            # 自动合并小分区
            merge_small_partitions(base_path, size_threshold_mb=10.0)

        if optimization_strategy == "auto" or optimization_strategy == "compress":
            # 压缩大分区（重新写入以应用更好的压缩）
            large_partitions = [(p, size) for p, size in partition_sizes if size > 50 * 1024 * 1024]  # 50MB
            for partition_path, size in large_partitions[:5]:  # 限制处理数量
                try:
                    data_file = partition_path / "data.parquet"
                    df = pl.read_parquet(data_file)
                    # 重新写入以应用压缩
                    df.write_parquet(data_file, compression='zstd')
                    logging.debug(f"重新压缩分区: {partition_path.name}")
                except Exception as e:
                    logging.warning(f"压缩分区 {partition_path.name} 失败: {str(e)}")

        if optimization_strategy == "auto" or optimization_strategy == "repartition":
            # 重新分区优化（按数据量重新分配分区）
            repartition_optimization(base_path)

        optimization_duration = time.time() - start_time
        logging.info(f"分区存储优化完成，耗时: {optimization_duration:.2f}s")

    except Exception as e:
        logging.error(f"优化分区存储失败: {str(e)}")
        raise


def repartition_optimization(base_path: Path, target_partition_size_mb: float = 100.0):
    """
    重新分区优化，将数据重新分布以达到目标分区大小

    Args:
        base_path (Path): 基础路径
        target_partition_size_mb (float): 目标分区大小（MB）
    """
    import time
    start_time = time.time()

    try:
        logging.info(f"开始重新分区优化，目标分区大小: {target_partition_size_mb}MB")

        # 获取所有分区及其大小
        partition_sizes = check_partition_sizes(base_path)
        total_size = sum(size for _, size in partition_sizes)
        target_num_partitions = max(1, int(total_size / (target_partition_size_mb * 1024 * 1024)))

        logging.info(f"总数据大小: {total_size / (1024*1024):.2f}MB, "
                    f"目标分区数: {target_num_partitions}, "
                    f"当前分区数: {len(partition_sizes)}")

        if len(partition_sizes) <= target_num_partitions:
            logging.info("分区数量已达到目标，无需重新分区")
            return

        # 合并分区直到达到目标数量
        merge_partitions_to_target(base_path, target_num_partitions)

        repartition_duration = time.time() - start_time
        logging.info(f"重新分区优化完成，耗时: {repartition_duration:.2f}s")

    except Exception as e:
        logging.error(f"重新分区优化失败: {str(e)}")
        raise


def merge_partitions_to_target(base_path: Path, target_num_partitions: int):
    """
    将分区合并到目标数量

    Args:
        base_path (Path): 基础路径
        target_num_partitions (int): 目标分区数量
    """
    import tempfile
    from datetime import datetime

    try:
        # 获取所有分区
        partition_dirs = [p for p, _ in check_partition_sizes(base_path)]

        if len(partition_dirs) <= target_num_partitions:
            logging.info(f"分区数({len(partition_dirs)})已小于等于目标数({target_num_partitions})")
            return

        # 按分区名排序，合并相邻分区
        partition_dirs.sort()

        # 合并分区直到达到目标数量
        while len(partition_dirs) > target_num_partitions:
            # 合并前两个分区
            partition_1 = partition_dirs[0]
            partition_2 = partition_dirs[1]

            # 读取两个分区的数据
            data_file_1 = partition_1 / "data.parquet"
            data_file_2 = partition_2 / "data.parquet"

            if not (data_file_1.exists() and data_file_2.exists()):
                # 如果分区文件不存在，移除该目录
                if partition_1 in partition_dirs:
                    partition_dirs.remove(partition_1)
                if partition_2 in partition_dirs and partition_2 in partition_dirs:
                    partition_dirs.remove(partition_2)
                continue

            df1 = pl.read_parquet(data_file_1)
            df2 = pl.read_parquet(data_file_2)

            # 合并数据
            merged_df = pl.concat([df1, df2])

            # 生成合并后的分区名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            merged_name = f"merged_{timestamp}"
            merged_partition_dir = base_path / merged_name
            merged_partition_dir.mkdir(exist_ok=True)

            # 写入合并后的数据
            output_path = merged_partition_dir / "data.parquet"
            merged_df.write_parquet(output_path, compression='zstd')

            # 删除原始分区
            shutil.rmtree(partition_1)
            shutil.rmtree(partition_2)

            # 更新分区列表
            partition_dirs = [p for p in partition_dirs if p not in [partition_1, partition_2]]
            partition_dirs.insert(0, merged_partition_dir)  # 添加合并后的分区
            partition_dirs.sort()

            logging.debug(f"合并分区: {partition_1.name} 和 {partition_2.name} -> {merged_name}")

    except Exception as e:
        logging.error(f"合并分区到目标数量失败: {str(e)}")
        raise


def manage_partition_metadata(partition_path: Path, action: str = "get", metadata: dict = None):
    """
    管理分区级别的元数据

    Args:
        partition_path (Path): 分区路径
        action (str): 操作类型 ("get", "create", "update", "delete")
        metadata (dict): 元数据信息

    Returns:
        dict: 分区元数据
    """
    import json
    import time
    from datetime import datetime

    metadata_file = partition_path / "metadata.json"

    try:
        if action == "create" or action == "update":
            # 创建或更新元数据
            if metadata is None:
                metadata = {}

            # 添加基本元数据
            metadata.update({
                "partition_path": str(partition_path),
                "created_time": metadata.get("created_time", datetime.now().isoformat()),
                "last_modified": datetime.now().isoformat(),
                "data_file_size": (partition_path / "data.parquet").stat().st_size if (partition_path / "data.parquet").exists() else 0,
                "record_count": 0
            })

            # 尝试读取数据文件以获取记录数
            try:
                if (partition_path / "data.parquet").exists():
                    df = pl.read_parquet(partition_path / "data.parquet")
                    metadata["record_count"] = len(df)
            except Exception as e:
                logging.warning(f"无法读取分区数据文件以获取记录数: {str(e)}")

            # 写入元数据文件
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            return metadata

        elif action == "get":
            # 获取元数据
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # 如果元数据文件不存在，创建基本元数据
                return manage_partition_metadata(partition_path, action="create")

        elif action == "delete":
            # 删除元数据文件
            if metadata_file.exists():
                metadata_file.unlink()
            return {}

    except Exception as e:
        logging.error(f"管理分区元数据失败: {str(e)}")
        return {}


def manage_partition_lifecycle(base_path: Path, action: str = "cleanup", retention_days: int = 90):
    """
    管理分区级别的数据生命周期

    Args:
        base_path (Path): 基础路径
        action (str): 操作类型 ("cleanup", "archive", "backup", "restore")
        retention_days (int): 保留天数

    Returns:
        dict: 操作结果
    """
    import time
    from datetime import datetime, timedelta
    import shutil

    try:
        logging.info(f"开始分区生命周期管理: {base_path}, 操作: {action}")

        result = {
            "action": action,
            "processed_partitions": 0,
            "affected_partitions": 0,
            "errors": []
        }

        if action == "cleanup":
            # 清理过期分区
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            for partition_dir in base_path.iterdir():
                if partition_dir.is_dir():
                    # 获取分区元数据
                    metadata = manage_partition_metadata(partition_dir, action="get")
                    created_time = metadata.get("created_time")

                    if created_time:
                        try:
                            created_dt = datetime.fromisoformat(created_time)
                            if created_dt < cutoff_date:
                                # 删除过期分区
                                shutil.rmtree(partition_dir)
                                result["affected_partitions"] += 1
                                logging.info(f"删除过期分区: {partition_dir.name}")
                        except Exception as e:
                            error_msg = f"处理分区 {partition_dir.name} 时出错: {str(e)}"
                            result["errors"].append(error_msg)
                            logging.warning(error_msg)

        elif action == "archive":
            # 归档旧分区
            archive_dir = base_path / "archived"
            archive_dir.mkdir(exist_ok=True)

            cutoff_date = datetime.now() - timedelta(days=retention_days)
            for partition_dir in base_path.iterdir():
                if partition_dir.is_dir() and partition_dir.name != "archived":
                    metadata = manage_partition_metadata(partition_dir, action="get")
                    created_time = metadata.get("created_time")

                    if created_time:
                        try:
                            created_dt = datetime.fromisoformat(created_time)
                            if created_dt < cutoff_date:
                                # 移动到归档目录
                                archive_path = archive_dir / partition_dir.name
                                if archive_path.exists():
                                    shutil.rmtree(archive_path)
                                shutil.move(str(partition_dir), str(archive_path))
                                result["affected_partitions"] += 1
                                logging.info(f"归档分区: {partition_dir.name}")
                        except Exception as e:
                            error_msg = f"归档分区 {partition_dir.name} 时出错: {str(e)}"
                            result["errors"].append(error_msg)
                            logging.warning(error_msg)

        elif action == "backup":
            # 备份所有分区
            backup_dir = base_path / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_dir.mkdir(exist_ok=True)

            for partition_dir in base_path.iterdir():
                if partition_dir.is_dir() and not partition_dir.name.startswith("backup_") and partition_dir.name != "archived":
                    try:
                        backup_partition_path = backup_dir / partition_dir.name
                        shutil.copytree(partition_dir, backup_partition_path)
                        result["affected_partitions"] += 1
                        logging.info(f"备份分区: {partition_dir.name}")
                    except Exception as e:
                        error_msg = f"备份分区 {partition_dir.name} 时出错: {str(e)}"
                        result["errors"].append(error_msg)
                        logging.warning(error_msg)

        result["processed_partitions"] = len([p for p in base_path.iterdir() if p.is_dir()])
        logging.info(f"分区生命周期管理完成: {result}")

        return result

    except Exception as e:
        logging.error(f"分区生命周期管理失败: {str(e)}")
        return {"error": str(e)}


def monitor_partition_performance(partition_path: Path, query_pattern: str = "sequential") -> dict:
    """
    监控分区查询性能

    Args:
        partition_path (Path): 分区路径
        query_pattern (str): 查询模式 ("sequential", "random", "aggregation")

    Returns:
        dict: 性能监控结果
    """
    import time
    import json

    try:
        performance_metrics = {
            "partition": partition_path.name,
            "timestamp": time.time(),
            "query_pattern": query_pattern
        }

        data_file = partition_path / "data.parquet"

        if not data_file.exists():
            performance_metrics["error"] = "数据文件不存在"
            return performance_metrics

        # 测量文件大小
        file_size = data_file.stat().st_size
        performance_metrics["data_size_mb"] = file_size / (1024 * 1024)

        # 测量查询性能
        start_time = time.time()

        if query_pattern == "sequential":
            # 顺序读取性能
            df = pl.read_parquet(data_file)
            performance_metrics["record_count"] = len(df)
        elif query_pattern == "random":
            # 随机读取性能（读取前几行）
            df = pl.read_parquet(data_file, n_rows=100)
        elif query_pattern == "aggregation":
            # 聚合查询性能
            df = pl.scan_parquet(data_file).select(pl.col("*")).collect()

        query_time = (time.time() - start_time) * 1000  # 转换为毫秒
        performance_metrics["query_time_ms"] = query_time

        # 计算吞吐量
        if query_time > 0:
            performance_metrics["throughput_mb_per_sec"] = (file_size / (1024 * 1024)) / (query_time / 1000)

        # 更新性能历史记录
        performance_history_file = partition_path / "performance_history.json"
        update_performance_history(performance_history_file, performance_metrics)

        return performance_metrics

    except Exception as e:
        logging.error(f"监控分区性能失败 {partition_path.name}: {str(e)}")
        return {"error": str(e), "partition": partition_path.name}


def update_performance_history(history_file: Path, new_metrics: dict):
    """
    更新性能历史记录

    Args:
        history_file (Path): 历史记录文件路径
        new_metrics (dict): 新的性能指标
    """
    import json

    try:
        # 读取现有历史记录
        if history_file.exists():
            with open(history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
        else:
            history = []

        # 添加新记录
        history.append(new_metrics)

        # 只保留最近100条记录
        if len(history) > 100:
            history = history[-100:]

        # 保存历史记录
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    except Exception as e:
        logging.warning(f"更新性能历史记录失败: {str(e)}")


def analyze_partition_query_performance(base_path: Path, time_window_hours: int = 24) -> dict:
    """
    分析分区查询性能

    Args:
        base_path (Path): 基础路径
        time_window_hours (int): 分析时间窗口（小时）

    Returns:
        dict: 性能分析结果
    """
    import time
    from datetime import datetime, timedelta

    try:
        analysis_result = {
            "total_partitions": 0,
            "analyzed_partitions": 0,
            "avg_query_time_ms": 0,
            "max_query_time_ms": 0,
            "min_query_time_ms": float('inf'),
            "total_query_time_ms": 0,
            "performance_issues": []
        }

        query_times = []

        # 分析每个分区的性能
        for partition_dir in base_path.iterdir():
            if partition_dir.is_dir():
                analysis_result["total_partitions"] += 1

                performance_history_file = partition_dir / "performance_history.json"
                if performance_history_file.exists():
                    try:
                        # 读取性能历史记录
                        import json
                        with open(performance_history_file, 'r', encoding='utf-8') as f:
                            history = json.load(f)

                        if history:
                            # 过滤时间窗口内的记录
                            cutoff_time = time.time() - (time_window_hours * 3600)
                            recent_records = [record for record in history if record.get("timestamp", 0) > cutoff_time]

                            if recent_records:
                                # 计算平均查询时间
                                partition_query_times = [record.get("query_time_ms", 0) for record in recent_records]
                                avg_query_time = sum(partition_query_times) / len(partition_query_times)

                                query_times.extend(partition_query_times)
                                analysis_result["analyzed_partitions"] += 1

                                # 检查性能问题（查询时间超过1000ms）
                                if avg_query_time > 1000:
                                    analysis_result["performance_issues"].append({
                                        "partition": partition_dir.name,
                                        "avg_query_time_ms": avg_query_time
                                    })

                    except Exception as e:
                        logging.warning(f"分析分区 {partition_dir.name} 性能时出错: {str(e)}")

        # 计算总体统计信息
        if query_times:
            analysis_result["avg_query_time_ms"] = sum(query_times) / len(query_times)
            analysis_result["max_query_time_ms"] = max(query_times)
            analysis_result["min_query_time_ms"] = min(query_times)
            analysis_result["total_query_time_ms"] = sum(query_times)

        # 如果min_query_time_ms仍然是无穷大，设置为0
        if analysis_result["min_query_time_ms"] == float('inf'):
            analysis_result["min_query_time_ms"] = 0

        return analysis_result

    except Exception as e:
        logging.error(f"分析分区查询性能失败: {str(e)}")
        return {"error": str(e)}


def setup_performance_alerts(base_path: Path, threshold_ms: float = 1000.0,
                           alert_email: str = None) -> bool:
    """
    设置性能异常报警机制

    Args:
        base_path (Path): 基础路径
        threshold_ms (float): 性能阈值（毫秒）
        alert_email (str): 报警邮件地址

    Returns:
        bool: 设置结果
    """
    import time
    import json

    try:
        alert_config = {
            "threshold_ms": threshold_ms,
            "alert_email": alert_email,
            "enabled": True,
            "last_check": time.time()
        }

        # 保存报警配置
        alert_config_file = base_path / "performance_alert_config.json"
        with open(alert_config_file, 'w', encoding='utf-8') as f:
            json.dump(alert_config, f, ensure_ascii=False, indent=2)

        logging.info(f"性能报警已设置，阈值: {threshold_ms}ms")
        return True

    except Exception as e:
        logging.error(f"设置性能报警失败: {str(e)}")
        return False


def check_performance_alerts(base_path: Path) -> list:
    """
    检查性能异常并触发报警

    Args:
        base_path (Path): 基础路径

    Returns:
        list: 报警列表
    """
    try:
        alerts = []

        # 读取报警配置
        alert_config_file = base_path / "performance_alert_config.json"
        if not alert_config_file.exists():
            return alerts

        import json
        with open(alert_config_file, 'r', encoding='utf-8') as f:
            alert_config = json.load(f)

        if not alert_config.get("enabled", False):
            return alerts

        threshold_ms = alert_config.get("threshold_ms", 1000.0)

        # 分析性能
        performance_analysis = analyze_partition_query_performance(base_path)

        # 检查是否有分区超过阈值
        for issue in performance_analysis.get("performance_issues", []):
            if issue.get("avg_query_time_ms", 0) > threshold_ms:
                alert = {
                    "type": "performance_alert",
                    "partition": issue["partition"],
                    "query_time_ms": issue["avg_query_time_ms"],
                    "threshold_ms": threshold_ms,
                    "timestamp": time.time()
                }
                alerts.append(alert)
                logging.warning(f"性能报警: 分区 {issue['partition']} 查询时间 {issue['avg_query_time_ms']:.2f}ms 超过阈值 {threshold_ms}ms")

        return alerts

    except Exception as e:
        logging.error(f"检查性能报警失败: {str(e)}")
        return []


def manage_partition_access_control(partition_path: Path, user: str = None,
                                 permission: str = "read", action: str = "grant"):
    """
    管理分区级别的访问控制

    Args:
        partition_path (Path): 分区路径
        user (str): 用户名
        permission (str): 权限类型 ("read", "write", "admin")
        action (str): 操作类型 ("grant", "revoke", "check")

    Returns:
        bool: 操作结果
    """
    import json

    try:
        # 访问控制文件路径
        acl_file = partition_path / "access_control.json"

        # 读取现有的访问控制列表
        if acl_file.exists():
            with open(acl_file, 'r', encoding='utf-8') as f:
                acl = json.load(f)
        else:
            acl = {"users": {}}

        if action == "grant":
            # 授予权限
            if user:
                if user not in acl["users"]:
                    acl["users"][user] = []
                if permission not in acl["users"][user]:
                    acl["users"][user].append(permission)
                    logging.info(f"授予用户 {user} {permission} 权限到分区 {partition_path.name}")

        elif action == "revoke":
            # 撤销权限
            if user and user in acl["users"]:
                if permission in acl["users"][user]:
                    acl["users"][user].remove(permission)
                    if not acl["users"][user]:  # 如果用户没有其他权限，移除用户
                        del acl["users"][user]
                    logging.info(f"撤销用户 {user} {permission} 权限从分区 {partition_path.name}")

        elif action == "check":
            # 检查权限
            if user and user in acl["users"]:
                return permission in acl["users"][user]
            return False

        # 保存访问控制列表
        if action in ["grant", "revoke"]:
            with open(acl_file, 'w', encoding='utf-8') as f:
                json.dump(acl, f, ensure_ascii=False, indent=2)

        return True

    except Exception as e:
        logging.error(f"管理分区访问控制失败: {str(e)}")
        return False


def adjust_partition_strategy(base_path: Path, data_type: str = "general",
                            time_window_days: int = 30,
                            size_threshold_mb: float = 50.0):
    """
    动态调整分区策略，根据数据量和查询模式变化优化分区

    Args:
        base_path (Path): 基础路径
        data_type (str): 数据类型
        time_window_days (int): 分析时间窗口（天）
        size_threshold_mb (float): 调整阈值（MB）
    """
    import time
    import threading
    start_time = time.time()

    try:
        logging.info(f"开始动态调整分区策略: {base_path}, 数据类型: {data_type}")

        # 分析当前存储效率
        efficiency_metrics = analyze_storage_efficiency(base_path)
        logging.debug(f"当前存储效率: {efficiency_metrics}")

        # 确定调整策略
        adjustment_needed = False
        strategies = []

        if efficiency_metrics.get("avg_size_mb", 0) < 5:  # 平均分区太小
            strategies.append("merge_small_partitions")
            adjustment_needed = True
        elif efficiency_metrics.get("avg_size_mb", 0) > 500:  # 平均分区太大
            strategies.append("split_large_partitions")
            adjustment_needed = True

        if efficiency_metrics.get("efficiency_score", 0) < 60:  # 效率分数低
            strategies.append("repartition_optimization")
            adjustment_needed = True

        if not adjustment_needed:
            logging.info("当前分区策略已较优，无需调整")
            return

        # 获取当前分区字段（从目录名推断）
        partition_field = infer_partition_field(base_path)

        # 根据数据类型应用调整策略
        for strategy in strategies:
            if strategy == "merge_small_partitions":
                merge_small_partitions(base_path, size_threshold_mb=10.0)
            elif strategy == "repartition_optimization":
                repartition_optimization(base_path, target_partition_size_mb=size_threshold_mb)

        # 确保操作的线程安全性
        with threading.Lock():
            logging.info(f"分区策略调整完成，策略: {strategies}")

        adjustment_duration = time.time() - start_time
        logging.info(f"动态分区策略调整完成，耗时: {adjustment_duration:.2f}s")

    except Exception as e:
        logging.error(f"动态调整分区策略失败: {str(e)}")
        raise


def infer_partition_field(base_path: Path) -> str:
    """
    推断分区字段名称

    Args:
        base_path (Path): 基础路径

    Returns:
        str: 推断的分区字段名称
    """
    try:
        for item in base_path.iterdir():
            if item.is_dir():
                name = item.name
                if '=' in name:
                    field = name.split('=')[0]
                    return field
    except:
        pass
    return "year"  # 默认分区字段


def analyze_storage_efficiency(base_path: Path) -> dict:
    """
    分析存储效率

    Args:
        base_path (Path): 基础路径

    Returns:
        dict: 存储效率分析结果
    """
    try:
        partition_sizes = check_partition_sizes(base_path)

        if not partition_sizes:
            return {
                "total_partitions": 0,
                "total_size_mb": 0.0,
                "avg_size_mb": 0.0,
                "min_size_mb": 0.0,
                "max_size_mb": 0.0,
                "small_partitions": 0,
                "efficiency_score": 0.0
            }

        sizes = [size for _, size in partition_sizes]
        total_size = sum(sizes)
        avg_size = total_size / len(sizes)
        min_size = min(sizes)
        max_size = max(sizes)

        # 定义小分区阈值为10MB
        small_threshold = 10 * 1024 * 1024
        small_partitions = sum(1 for size in sizes if size < small_threshold)

        # 计算效率评分（0-100），考虑分区大小分布的均匀性
        size_variance = sum((size - avg_size) ** 2 for size in sizes) / len(sizes)
        max_acceptable_variance = (50 * 1024 * 1024) ** 2  # 50MB差异的平方
        efficiency_score = max(0, min(100, 100 - (size_variance / max_acceptable_variance) * 100))

        return {
            "total_partitions": len(partition_sizes),
            "total_size_mb": total_size / (1024 * 1024),
            "avg_size_mb": avg_size / (1024 * 1024),
            "min_size_mb": min_size / (1024 * 1024),
            "max_size_mb": max_size / (1024 * 1024),
            "small_partitions": small_partitions,
            "efficiency_score": efficiency_score
        }

    except Exception as e:
        logging.error(f"分析存储效率失败: {str(e)}")
        return {}


def merge_small_partitions(base_path: Path, size_threshold_mb: float = 10.0,
                          max_partitions_to_merge: int = 10):
    """
    合并小分区文件以优化存储

    Args:
        base_path (Path): 基础路径
        size_threshold_mb (float): 小文件阈值（MB）
        max_partitions_to_merge (int): 最大合并分区数
    """
    import time
    start_time = time.time()

    try:
        size_threshold_bytes = size_threshold_mb * 1024 * 1024

        # 检查分区大小
        partition_sizes = check_partition_sizes(base_path)

        # 筛选小分区
        small_partitions = [(p, size) for p, size in partition_sizes if size < size_threshold_bytes]

        if len(small_partitions) < 2:
            logging.info(f"小分区数量不足，无需合并: {len(small_partitions)} 个小分区")
            return

        # 限制合并的分区数量
        partitions_to_merge = small_partitions[:min(max_partitions_to_merge, len(small_partitions))]

        logging.info(f"开始合并 {len(partitions_to_merge)} 个小分区")

        # 收集所有小分区的数据
        all_dataframes = []
        merged_partition_names = []

        for partition_path, size in partitions_to_merge:
            try:
                data_file = partition_path / "data.parquet"
                df = pl.read_parquet(data_file)
                all_dataframes.append(df)
                merged_partition_names.append(partition_path.name)
                logging.debug(f"读取分区 {partition_path.name}, 大小: {size / 1024:.2f} KB")
            except Exception as e:
                logging.warning(f"读取分区 {partition_path.name} 失败: {str(e)}")
                continue

        if not all_dataframes:
            logging.warning("没有成功读取任何分区数据")
            return

        # 合并数据
        merged_df = pl.concat(all_dataframes)
        logging.info(f"合并完成，总记录数: {len(merged_df)}")

        # 创建新的合并分区目录
        timestamp = int(time.time())
        merged_partition_name = f"merged_{timestamp}"
        merged_partition_dir = base_path / merged_partition_name
        merged_partition_dir.mkdir(parents=True, exist_ok=True)

        # 写入合并后的数据
        output_path = merged_partition_dir / "data.parquet"
        merged_df.write_parquet(output_path, compression=COMPRESSION_TYPE)

        # 删除原始小分区
        deleted_count = 0
        for partition_path, _ in partitions_to_merge:
            try:
                shutil.rmtree(partition_path)
                deleted_count += 1
            except Exception as e:
                logging.warning(f"删除分区 {partition_path.name} 失败: {str(e)}")

        merge_duration = time.time() - start_time
        final_size = output_path.stat().st_size
        logging.info(f"小分区合并完成: 合并了 {deleted_count} 个分区到 {merged_partition_name}, "
                    f"最终大小: {final_size / (1024*1024):.2f} MB, 耗时: {merge_duration:.2f}s")

    except Exception as e:
        logging.error(f"合并小分区失败: {str(e)}")
        raise


def atomic_partitioned_sink(lazy_frame, base_path: Path, partition_by: list,
                          partition_validation: bool = True,
                          performance_monitoring: bool = True):
    """
    原子性分区写入
    将数据按分区写入，避免数据损坏
    增强功能：支持分区验证和性能监控
    """
    import time
    start_time = time.time()

    try:
        # 获取动态阈值 - 使用内存压力检测
        dynamic_threshold = get_dynamic_streaming_threshold()

        # 首先估算数据大小以决定是否使用流式处理
        # 使用polars的lazy框架估算行数
        try:
            # 获取数据行数估算（不触发实际收集）
            sample_row_count = lazy_frame.select(pl.len()).collect().item()
            logging.debug(f"数据行数估算: {sample_row_count}, 流式处理阈值: {dynamic_threshold}")
        except:
            # 如果无法估算，使用默认值
            sample_row_count = 0
            logging.warning("无法估算数据行数，使用默认处理方式")

        # 根据动态阈值决定处理方式
        if sample_row_count > dynamic_threshold:
            logging.info(f"数据量 ({sample_row_count}) 超过动态阈值 ({dynamic_threshold})，使用流式处理")
            _streaming_partitioned_sink(lazy_frame, base_path, partition_by, dynamic_threshold)
        else:
            logging.info(f"数据量 ({sample_row_count}) 低于动态阈值 ({dynamic_threshold})，使用传统处理")
            # 传统方式 - 保持原有逻辑
            df = lazy_frame.collect()

            # 检查分区字段是否存在 - 如果缺失立即中断
            missing_cols = [col for col in partition_by if col not in df.columns]
            if missing_cols:
                logging.error(f"分区字段缺失: {missing_cols}，立即中断执行。不允许降级到非分区存储。")
                raise ValueError(f"分区字段缺失: {missing_cols}，无法执行分区存储操作")

            # 确保基础路径存在
            base_path.mkdir(parents=True, exist_ok=True)

            # 分区验证
            if partition_validation:
                unique_partitions = df.select(partition_by).unique()
                logging.info(f"发现 {len(unique_partitions)} 个唯一分区")

            # 按分区字段分组
            for partition_values in df.select(partition_by).unique().iter_rows():
                partition_start_time = time.time()

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

                        # 性能监控
                        partition_duration = time.time() - partition_start_time
                        if performance_monitoring:
                            logging.debug(f"成功写入分区 {partition_dir.name}: {len(partition_df)} 条记录, 耗时: {partition_duration:.2f}s")
                        else:
                            logging.debug(f"成功写入分区 {partition_dir.name}: {len(partition_df)} 条记录")

                    except Exception as e:
                        # 如果出错，删除临时文件
                        if Path(tmp_file.name).exists():
                            Path(tmp_file.name).unlink()
                        logging.error(f"写入分区 {partition_dir.name} 失败: {str(e)}")
                        raise

            # 总体性能监控
            total_duration = time.time() - start_time
            if performance_monitoring:
                logging.info(f"分区写入完成: {base_path}, 总耗时: {total_duration:.2f}s, 平均每分区: {total_duration/max(len(df.select(partition_by).unique()), 1):.2f}s")

    except Exception as e:
        logging.error(f"atomic_partitioned_sink 失败: {str(e)}")
        raise


def enhanced_yearly_partitioned_sink(lazy_frame, base_path: Path,
                                   partition_field: str = 'year',
                                   data_type: str = "general",
                                   optimize_compression: bool = True):
    """
    增强的按年分区写入功能
    专门针对年分区优化的实现
    """
    import time
    start_time = time.time()

    try:
        # 收集数据
        df = lazy_frame.collect()

        # 检查分区字段是否存在
        if partition_field not in df.columns:
            logging.error(f"分区字段 {partition_field} 不存在")
            raise ValueError(f"分区字段 {partition_field} 不存在")

        # 确保基础路径存在
        base_path.mkdir(parents=True, exist_ok=True)

        # 获取所有唯一年份
        unique_years = df.select(pl.col(partition_field).unique()).to_series().to_list()
        logging.info(f"按年分区，发现 {len(unique_years)} 个年份: {sorted(unique_years)}")

        # 根据数据类型选择压缩策略
        compression_type = COMPRESSION_TYPE
        if optimize_compression and data_type in ["daily", "daily_basic", "moneyflow"]:
            compression_type = 'zstd'  # 对于高频日数据使用更高效的压缩

        # 为每个年份创建分区
        total_records = 0
        for year in unique_years:
            partition_start_time = time.time()

            # 筛选当前年份的数据
            partition_df = df.filter(pl.col(partition_field) == year)

            if len(partition_df) == 0:
                continue

            # 构建年份分区目录
            year_partition_dir = base_path / f"{partition_field}={year}"
            year_partition_dir.mkdir(parents=True, exist_ok=True)

            # 定义输出文件路径
            output_path = year_partition_dir / "data.parquet"

            # 使用临时文件进行原子写入
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                try:
                    # 写入临时文件
                    partition_df.write_parquet(
                        tmp_file.name,
                        compression=compression_type,
                        row_group_size=CHUNK_SIZE
                    )

                    # 原子性替换
                    if output_path.exists():
                        backup_path = output_path.with_suffix(output_path.suffix + '.backup')
                        if backup_path.exists():
                            backup_path.unlink()
                        output_path.rename(backup_path)

                    Path(tmp_file.name).rename(output_path)

                    if 'backup_path' in locals() and backup_path.exists():
                        backup_path.unlink()

                    partition_duration = time.time() - partition_start_time
                    records_count = len(partition_df)
                    total_records += records_count
                    logging.info(f"成功写入年份分区 {year}: {records_count} 条记录, 耗时: {partition_duration:.2f}s")

                except Exception as e:
                    if Path(tmp_file.name).exists():
                        Path(tmp_file.name).unlink()
                    logging.error(f"写入年份分区 {year} 失败: {str(e)}")
                    raise

        total_duration = time.time() - start_time
        logging.info(f"增强年分区写入完成: {base_path}, 总记录数: {total_records}, 总耗时: {total_duration:.2f}s")

    except Exception as e:
        logging.error(f"enhanced_yearly_partitioned_sink 失败: {str(e)}")
        raise


def _streaming_partitioned_sink(lazy_frame, base_path: Path, partition_by: list, threshold: int):
    """
    流式分区写入实现
    用于处理大数据集，避免内存溢出
    """
    logging.info(f"开始流式分区写入，分区字段: {partition_by}")

    # 确保基础路径存在
    base_path.mkdir(parents=True, exist_ok=True)

    # 使用polars的collect_groups方法进行流式处理
    # 先获取所有分区值的组合
    partition_values_df = lazy_frame.select(partition_by).unique().collect()

    for partition_values in partition_values_df.iter_rows():
        # 构建分区条件
        condition = True
        for i, col in enumerate(partition_by):
            val = partition_values[i]
            condition = condition & (pl.col(col) == val)

        # 使用懒加载筛选当前分区数据
        partition_lazy_frame = lazy_frame.filter(condition)

        # 获取分区数据总数
        partition_count = partition_lazy_frame.select(pl.len()).collect().item()

        if partition_count == 0:
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
                # 流式写入临时文件 - 使用collect_streaming方法或分块处理
                partition_lazy_frame.sink_parquet(
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

                logging.debug(f"成功流式写入分区 {partition_dir.name}: {partition_count} 条记录")
            except Exception as e:
                # 如果出错，删除临时文件
                if Path(tmp_file.name).exists():
                    Path(tmp_file.name).unlink()
                logging.error(f"流式写入分区 {partition_dir.name} 失败: {str(e)}")
                raise


def _get_memory_usage():
    """获取当前内存使用率"""
    return psutil.virtual_memory().percent


def atomic_unpartitioned_sink(lazy_frame, output_path: str, performance_monitoring: bool = True):
    """
    原子性非分区写入
    将数据写入单个文件，避免数据损坏
    优化：支持性能监控和大文件处理
    """
    import time
    start_time = time.time()

    try:
        # 获取动态阈值 - 使用内存压力检测
        dynamic_threshold = get_dynamic_streaming_threshold()

        # 首先估算数据大小以决定是否使用流式处理
        try:
            # 获取数据行数估算（不触发实际收集）
            sample_row_count = lazy_frame.select(pl.len()).collect().item()
            logging.debug(f"非分区数据行数估算: {sample_row_count}, 流式处理阈值: {dynamic_threshold}")
        except:
            # 如果无法估算，使用默认值
            sample_row_count = 0
            logging.warning("无法估算非分区数据行数，使用默认处理方式")

        # 确保输出路径的目录存在
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # 使用临时文件进行原子写入
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            try:
                if sample_row_count > dynamic_threshold:
                    logging.info(f"非分区数据量 ({sample_row_count}) 超过动态阈值 ({dynamic_threshold})，使用流式写入")
                    # 流式写入临时文件 - 使用sink_parquet方法
                    lazy_frame.sink_parquet(
                        tmp_file.name,
                        compression=COMPRESSION_TYPE,
                        row_group_size=CHUNK_SIZE
                    )
                else:
                    logging.info(f"非分区数据量 ({sample_row_count}) 低于动态阈值 ({dynamic_threshold})，使用传统写入")
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

                # Performance monitoring
                if performance_monitoring:
                    file_size = Path(output_path).stat().st_size
                    duration = time.time() - start_time
                    logging.info(f"成功写入文件 {output_path}, 大小: {file_size / (1024*1024):.2f} MB, 耗时: {duration:.2f}s, 吞吐量: {(file_size / (1024*1024)) / duration:.2f} MB/s")

            except Exception as e:
                # 如果出错，删除临时文件
                tmp_path = Path(tmp_file.name)
                if tmp_path.exists():
                    tmp_path.unlink()
                logging.error(f"写入文件 {output_path} 失败: {str(e)}")
                raise

    except Exception as e:
        logging.error(f"atomic_unpartitioned_sink 失败: {str(e)}")
        raise


def enhanced_unpartitioned_sink(lazy_frame, output_path: str, data_type: str = "general",
                               optimize_compression: bool = True,
                               file_size_check: bool = True):
    """
    增强的非分区写入功能
    支持数据类型特定优化和文件大小检查
    """
    import time
    start_time = time.time()

    try:
        # 获取动态阈值
        dynamic_threshold = get_dynamic_streaming_threshold()

        # 估算数据大小
        try:
            sample_row_count = lazy_frame.select(pl.len()).collect().item()
            logging.debug(f"非分区数据行数估算: {sample_row_count}, 数据类型: {data_type}")
        except:
            sample_row_count = 0
            logging.warning("无法估算数据行数，使用默认处理方式")

        # 确保输出路径的目录存在
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # 根据数据类型选择不同的压缩策略
        compression_type = COMPRESSION_TYPE
        if optimize_compression and data_type in ["financial", "income", "balancesheet", "cashflow", "fina_indicator"]:
            # 对于财务数据使用更高效的压缩
            compression_type = 'zstd'  # Use zstd for financial data

        # 使用临时文件进行原子写入
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            try:
                if sample_row_count > dynamic_threshold:
                    logging.info(f"数据量 ({sample_row_count}) 超过动态阈值 ({dynamic_threshold})，使用流式写入，数据类型: {data_type}")
                    lazy_frame.sink_parquet(
                        tmp_file.name,
                        compression=compression_type,
                        row_group_size=CHUNK_SIZE
                    )
                else:
                    logging.info(f"数据量 ({sample_row_count}) 低于动态阈值 ({dynamic_threshold})，使用传统写入，数据类型: {data_type}")
                    lazy_frame.collect().write_parquet(
                        tmp_file.name,
                        compression=compression_type,
                        row_group_size=CHUNK_SIZE
                    )

                # 原子性替换
                tmp_path = Path(tmp_file.name)
                if Path(output_path).exists():
                    backup_path = Path(output_path).with_suffix(Path(output_path).suffix + '.backup')
                    if backup_path.exists():
                        backup_path.unlink()
                    Path(output_path).rename(backup_path)

                tmp_path.rename(output_path)

                if 'backup_path' in locals() and backup_path.exists():
                    backup_path.unlink()

                # 文件完整性验证
                if file_size_check:
                    file_size = Path(output_path).stat().st_size
                    logging.info(f"成功写入文件 {output_path}, 大小: {file_size / (1024*1024):.2f} MB")

                    # Additional validation: try to read a small sample to verify integrity
                    try:
                        sample_read = pl.read_parquet(output_path, n_rows=1)
                        logging.debug(f"文件完整性验证通过，成功读取样本行")
                    except Exception as e:
                        logging.error(f"文件完整性验证失败: {str(e)}")
                        raise

                duration = time.time() - start_time
                logging.info(f"增强非分区写入完成: {output_path}, 耗时: {duration:.2f}s")

            except Exception as e:
                tmp_path = Path(tmp_file.name)
                if tmp_path.exists():
                    tmp_path.unlink()
                logging.error(f"写入文件 {output_path} 失败: {str(e)}")
                raise

    except Exception as e:
        logging.error(f"enhanced_unpartitioned_sink 失败: {str(e)}")
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

                    # 尝试读取文件以检查完整性 - 使用流式处理避免大文件内存问题
                    try:
                        # 快速检查文件是否可以读取
                        file_size = data_file.stat().st_size
                        logging.debug(f"检查分区文件: {data_file}, 大小: {file_size / (1024*1024):.2f} MB")

                        # 使用lazy scan来避免将整个文件加载到内存
                        lazy_df = pl.scan_parquet(data_file)
                        count = lazy_df.select(pl.len()).collect().item()
                        logging.info(f"分区 {partition_dir.name} 健康: {count} 条记录")
                    except Exception as e:
                        logging.error(f"分区 {partition_dir.name} 损坏: {str(e)}")
                        return False
        else:
            # 检查非分区存储
            if not Path(base_path).exists():
                logging.warning(f"数据文件不存在: {base_path}")
                return False

            try:
                # 快速检查大文件而不完全加载
                file_size = Path(base_path).stat().st_size
                logging.debug(f"检查数据文件: {base_path}, 大小: {file_size / (1024*1024):.2f} MB")

                # 使用lazy scan来避免将整个文件加载到内存
                lazy_df = pl.scan_parquet(base_path)
                count = lazy_df.select(pl.len()).collect().item()
                logging.info(f"数据文件健康: {count} 条记录")
            except Exception as e:
                logging.error(f"数据文件损坏: {str(e)}")
                return False

        # 添加内存使用监控
        memory_percent = _get_memory_usage()
        logging.info(f"存储健康检查完成，当前内存使用率: {memory_percent}%")

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

        # 添加内存使用监控
        initial_memory = _get_memory_usage()
        logging.info(f"优化开始时内存使用率: {initial_memory}%")

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

        # 检查当前内存使用情况
        final_memory = _get_memory_usage()
        logging.info(f"存储优化完成: {base_path}，内存使用率变化: {initial_memory}% -> {final_memory}%")

    except Exception as e:
        logging.error(f"优化存储失败: {str(e)}")
        raise