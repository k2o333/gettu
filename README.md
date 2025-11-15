# 全自动数据扫描与智能增量构建系统

## 概述

本系统实现了全自动数据扫描与智能增量构建功能，显著提高数据下载效率，避免重复下载已有数据，并智能决定对每个股票/数据段需要下载的时间范围。

## 主要功能

1. **自动扫描**：扫描 `/home/quan/testdata/aspipe/data` 目录，了解已有数据的时间覆盖范围
2. **智能决策**：根据扫描结果，决定对每个股票/数据段需要下载的时间范围
3. **效率优化**：避免重复下载已有数据，按需补充缺失部分
4. **数据完整性检查**：检查同一股票数据文件内部的时间缺口
5. **并发控制**：防止并发下载冲突

## 新增功能

### 1. 两种构建模式

- **`initial_build`**：传统的完整构建模式，按照指定日期范围下载所有数据
- **`initial_build_enhanced`**：智能增强构建模式，自动扫描现有数据并智能决定下载策略

```bash
# 使用传统构建模式（完整下载指定日期范围内的数据）
python main.py initial_build

# 使用增强构建模式（智能扫描和增量下载）
python main.py initial_build_enhanced

# 指定数据类型进行传统构建
python main.py initial_build daily,moneyflow

# 指定数据类型进行增强构建
python main.py initial_build_enhanced daily,moneyflow

# 指定数据类型和日期范围
python main.py initial_build daily,moneyflow 20050101 20231231
python main.py initial_build_enhanced daily,moneyflow 20050101 20231231

# 指定起始日期（默认到今天）
python main.py initial_build 20050101
python main.py initial_build_enhanced 20050101
```

### 2. 数据完整性检查

- 检查文件内部数据的连续性
- 识别时间序列中的缺口
- 智能处理缺失数据的补充

### 3. 智能下载决策

- 识别已有数据范围
- 计算需要补充的时间段
- 处理数据文件内部的时间缺口
- 避免重复下载

## 核心组件

### `enhanced_scanner.py`
- `DataIntegrityChecker`: 数据完整性检查器
- `DataScanner`: 自动扫描引擎
- `DownloadLockManager`: 并发下载锁管理器
- `DownloadDecisionEngine`: 智能下载决策引擎

## 使用建议

1. **首次使用**: 推荐使用 `initial_build_enhanced` 模式，可自动扫描现有数据并智能决定下载策略
2. **已有数据**: 增强版模式会自动检测已有数据范围，避免重复下载
3. **效率提升**: 通过数据扫描和完整性检查，减少不必要的数据下载

## 系统优势

- **数据完整性**: 不仅检查分区是否存在，还检查文件内数据的完整性和连续性
- **性能优化**: 实现扫描结果缓存和大文件采样处理
- **并发安全**: 下载锁机制防止并发下载冲突
- **智能决策**: 基于现有数据覆盖情况智能决定下载策略
- **错误处理**: 完善的异常处理和边界情况处理