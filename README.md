# 配置驱动数据管道系统 (app2)

## 项目概述

这是一个基于配置驱动架构的金融数据管道系统，用于从TuShare API下载、处理和存储中国股票市场的各种数据。该系统采用高内聚、低耦合、原子化的设计原则，通过统一的配置中心管理所有数据接口，实现了零代码扩展能力。

## 架构特点

### 1. 配置驱动
- 所有数据接口配置集中管理在 `config.py` 中
- 通过 `DATA_INTERFACE_CONFIG` 字典定义所有数据接口的参数
- 新增数据类型仅需在配置文件中添加相应条目

### 2. 模块化设计
- `config.py`: 统一数据接口配置中心
- `interface_manager.py`: 接口管理器（动态生成下载函数）
- `etl_runtime.py`: ETL运行时（配置驱动的数据处理）
- `scheduler_runtime.py`: 调度运行时（配置驱动的任务调度）
- `storage.py`: 存储模块（原子性数据写入）
- `metadata.py`: 元数据管理（SQLite数据库）
- `dictionaries.py`: 字典管理（股票、行业、地区等）
- `rate_limiter.py`: API限频管理
- `main.py`: 主程序入口

### 3. 高内聚低耦合
- 每个模块职责单一，通过配置字典通信
- 模块间无硬编码依赖
- 数据路径保持不变，确保数据连续性

## 依赖项

- Python 3.8+
- polars
- pandas
- tushare
- psutil
- python-dotenv

## 安装和配置

### 1. 环境准备
```bash
# 使用指定的conda环境
conda activate /root/miniforge3/envs/get

# 安装依赖
pip install -r requirements.txt
```

### 2. API密钥配置
在项目根目录创建 `.env` 文件：
```env
TUSHARE_TOKEN=your_tushare_token_here
```

### 3. 目录结构
确保以下目录结构存在：
```
/home/quan/testdata/aspipe/
├── app2/                   # 重构后的应用代码
│   ├── config.py
│   ├── interface_manager.py
│   ├── etl_runtime.py
│   ├── scheduler_runtime.py
│   ├── storage.py
│   ├── metadata.py
│   ├── dictionaries.py
│   ├── rate_limiter.py
│   ├── main.py
│   └── requirements.txt
├── data/                   # 数据目录（保持不变）
│   ├── daily/
│   ├── snapshots/
│   └── ...
└── p/                    # 文档目录
    └── p1x6.md
```

## 使用方法

### 1. 日常运行
```bash
cd /home/quan/testdata/aspipe/app2
python main.py daily_update
```

### 2. 初始数据构建
```bash
# 构建所有数据类型（默认从2005年1月1日开始到今天）
python main.py initial_build

# 构建指定数据类型
python main.py initial_build daily,moneyflow,block_trade

# 构建指定数据类型和日期范围
python main.py initial_build daily,moneyflow 20100101 20201231
```

### 3. 测试模式
```bash
# 测试所有数据字段的第一期下载
python main.py --test
```

### 4. 智能更新
系统会在运行 daily_update 时自动检测缺失的数据，并从第一个缺失日期开始下载，确保数据的连续性。

### 3. 各模块功能

#### 配置中心 (config.py)
定义了所有数据接口的统一配置，包括：
- API函数名和参数
- 参数支持能力
- 存储配置
- 更新频率和策略
- API限制

#### 接口管理器 (interface_manager.py)
- 根据配置动态生成下载函数
- 统一的参数构建逻辑
- 通用下载函数 `download_data_by_config`

#### ETL运行时 (etl_runtime.py)
- 配置驱动的数据处理流程
- 字段标准化和类型优化
- 按配置的分区策略存储数据

#### 调度运行时 (scheduler_runtime.py)
- 每日数据更新
- 初始数据构建
- 多种批量构建策略

## 扩展新数据类型

添加新数据类型仅需在 `config.py` 的 `DATA_INTERFACE_CONFIG` 字典中添加相应配置：

```python
'new_data_type': {
    'api_name': 'new_api_function',
    'download_func': 'new_api_function',
    'api_params': {'param1': 'value1'},
    'supports': {
        'ts_code': True,
        'start_date': False,
        'end_date': False,
        'trade_date': True,
        'period': False
    },
    'storage': {
        'path': EVENTS_DIR / 'new_data_type',
        'partition_granularity': PartitionGranularity.YEAR_MONTH,
        'partition_field': 'trade_date',
        'sort_fields': ['ts_code_id', 'trade_date_int']
    },
    'update': {
        'frequency': 'daily',
        'batch_strategy': 'by_date',
        'batch_size': 100
    },
    'api_limit': 200
}
```

无需修改任何其他模块，系统会自动处理新的数据类型。

## 数据存储策略

### 分区策略
- `YEAR`: 按年分区，适用于日线数据
- `YEAR_MONTH`: 按年月分区，适用于事件数据
- `NONE`: 无分区，适用于字典数据

### 路径映射
所有数据存储路径在 `config.py` 中定义，保持与原系统兼容。

## 日志和监控

系统自动生成日志文件在 `logs/` 目录下，按日期轮转保存7天。

## 错误处理和重试

- API调用包含重试机制（默认3次）
- 指数退避策略
- 限频控制防止API调用超限

## 维护建议

1. 定期检查API配额使用情况
2. 监控存储空间使用
3. 根据需要调整批处理大小以优化性能
4. 定期备份元数据数据库

## 性能优化

1. 利用28核CPU进行并行处理
2. 500万行以上数据采用流式处理
3. 使用ZSTD压缩算法优化存储
4. 智能分区策略减少查询时间