"""
智能分区策略选择器
根据数据类型和特征自动选择最优的分区策略
"""

import logging
from enum import Enum
from typing import Dict, Any
from config import DATA_INTERFACE_CONFIG

class PartitionStrategy(Enum):
    """分区策略枚举"""
    NONE = "none"           # 无分区（财务数据）
    YEAR = "year"           # 按年分区（日市场数据）
    YEAR_MONTH = "year_month"  # 按月分区（高频事件数据）
    MERGE = "merge"         # 合并策略（低频事件数据）

class PartitionStrategySelector:
    """智能分区策略选择器"""

    def __init__(self):
        """初始化策略选择器"""
        # 定义数据类型到策略的映射
        self.data_type_strategy_map = self._build_strategy_map()

    def _build_strategy_map(self) -> Dict[str, PartitionStrategy]:
        """构建数据类型到分区策略的映射"""
        strategy_map = {}

        # 根据配置文件中的存储配置自动分类
        for data_type, config in DATA_INTERFACE_CONFIG.items():
            storage_config = config.get('storage', {})
            granularity = storage_config.get('partition_granularity', 'none')

            # 处理枚举值
            if hasattr(granularity, 'value'):
                granularity = granularity.value

            if granularity == 'none':
                strategy_map[data_type] = PartitionStrategy.NONE
            elif granularity == 'year':
                strategy_map[data_type] = PartitionStrategy.YEAR
            elif granularity == 'year_month':
                strategy_map[data_type] = PartitionStrategy.YEAR_MONTH
            else:
                # 默认策略
                strategy_map[data_type] = PartitionStrategy.NONE

        return strategy_map

    def select_strategy(self, data_type: str) -> PartitionStrategy:
        """
        根据数据类型选择分区策略

        Args:
            data_type (str): 数据类型

        Returns:
            PartitionStrategy: 选择的分区策略
        """
        # 首先检查预定义的映射
        if data_type in self.data_type_strategy_map:
            return self.data_type_strategy_map[data_type]

        # 如果没有找到预定义策略，根据数据特征动态选择
        return self._select_dynamic_strategy(data_type)

    def _select_dynamic_strategy(self, data_type: str) -> PartitionStrategy:
        """
        根据数据特征动态选择分区策略

        Args:
            data_type (str): 数据类型

        Returns:
            PartitionStrategy: 选择的分区策略
        """
        try:
            # 获取数据配置
            if data_type not in DATA_INTERFACE_CONFIG:
                logging.warning(f"未知的数据类型: {data_type}, 使用默认策略 NONE")
                return PartitionStrategy.NONE

            config = DATA_INTERFACE_CONFIG[data_type]
            update_config = config.get('update', {})
            frequency = update_config.get('frequency', 'daily')

            # 根据更新频率选择策略
            if frequency == 'daily':
                # 日更新数据使用年分区
                return PartitionStrategy.YEAR
            elif frequency == 'monthly':
                # 月更新数据使用月分区或合并策略
                return PartitionStrategy.MERGE
            elif frequency == 'weekly':
                # 周更新数据使用年分区
                return PartitionStrategy.YEAR
            else:
                # 默认使用无分区
                return PartitionStrategy.NONE

        except Exception as e:
            logging.error(f"动态选择分区策略时出错: {str(e)}")
            # 出错时使用默认策略
            return PartitionStrategy.NONE

    def get_strategy_description(self, strategy: PartitionStrategy) -> str:
        """
        获取策略描述

        Args:
            strategy (PartitionStrategy): 分区策略

        Returns:
            str: 策略描述
        """
        descriptions = {
            PartitionStrategy.NONE: "无分区存储（适用于财务数据）",
            PartitionStrategy.YEAR: "按年分区存储（适用于日市场数据）",
            PartitionStrategy.YEAR_MONTH: "按月分区存储（适用于高频事件数据）",
            PartitionStrategy.MERGE: "合并存储策略（适用于低频事件数据）"
        }
        return descriptions.get(strategy, "未知策略")

# 全局单例实例
_partition_strategy_selector = None

def get_partition_strategy_selector() -> PartitionStrategySelector:
    """
    获取分区策略选择器单例实例

    Returns:
        PartitionStrategySelector: 分区策略选择器实例
    """
    global _partition_strategy_selector
    if _partition_strategy_selector is None:
        _partition_strategy_selector = PartitionStrategySelector()
    return _partition_strategy_selector