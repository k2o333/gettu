import os
from datetime import datetime
from pathlib import Path
import logging
from config import APP_DIR, DATA_INTERFACE_CONFIG
from metadata import init_metadata_db, get_last_update_date
from dictionaries import create_dictionaries
from scheduler_runtime import SchedulerRuntime
from interface_manager import download_data_by_config
import sys
from concurrent_downloader import OptimizedDataDownloader

def setup_logging():
    """设置日志系统"""
    try:
        log_dir = APP_DIR / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)

        # 创建logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # 创建文件处理器（每天一个日志文件，最多保留7天）
        from logging.handlers import TimedRotatingFileHandler
        file_handler = TimedRotatingFileHandler(
            log_dir / 'app.log',
            when='midnight',
            interval=1,
            backupCount=7
        )
        file_handler.setLevel(logging.INFO)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 创建格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 添加处理器到logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logging.info("日志系统初始化完成")

    except Exception as e:
        print(f"日志系统初始化失败: {str(e)}")
        raise


def initialize_system():
    """初始化系统"""
    logging.info("开始系统初始化...")
    
    # 确保数据目录存在
    from config import ROOT_DIR
    ROOT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 初始化元数据数据库
    init_metadata_db()
    
    # 创建字典
    create_dictionaries()
    
    logging.info("系统初始化完成")


def run_daily_update_concurrent():
    """多线程并发进行每日数据更新"""
    logging.info("开始多线程并发每日数据更新...")

    downloader = OptimizedDataDownloader(max_workers=10)
    try:
        downloader.download_all_data_daily_update()
        logging.info("所有数据字段每日更新完成")
    except Exception as e:
        logging.error(f"并发每日更新失败: {str(e)}")
        raise
    finally:
        downloader.close()


def run_daily_update():
    """运行每日更新（保持向后兼容性）"""
    run_daily_update_concurrent()


def run_initial_build(data_types=None, start_date='20050101', end_date=None):
    """运行初始数据构建，使用指定的起始和结束日期"""
    logging.info(f"开始初始数据构建 (日期范围: {start_date} - {end_date or 'today'})...")
    
    try:
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        # 使用自定义的初始构建函数，而不是SchedulerRuntime默认的
        # 这里我们会为每个数据类型使用指定的时间范围
        from custom_build import build_with_date_range
        build_with_date_range(data_types, start_date, end_date)
        
        logging.info("初始数据构建完成")
    except Exception as e:
        logging.error(f"初始数据构建失败: {str(e)}")
        raise


def run_test_concurrent():
    """多线程并发测试所有数据字段的下载"""
    logging.info("开始多线程并发测试所有数据字段的下载...")

    downloader = OptimizedDataDownloader(max_workers=10)
    try:
        downloader.download_all_data_test()
        logging.info("所有数据字段测试下载完成")
    except Exception as e:
        logging.error(f"并发测试下载失败: {str(e)}")
        raise
    finally:
        downloader.close()


def run_test():
    """测试所有数据字段的第一期下载 - 从2019年开始（保持向后兼容性）"""
    run_test_concurrent()


def main():
    """主函数"""
    setup_logging()
    initialize_system()

    # 可以根据命令行参数决定运行哪种模式
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == 'daily_update':
            # 使用多线程运行并发每日更新
            run_daily_update_concurrent()
        elif mode == 'initial_build':
            start_date = '20050101'  # 默认从2005年1月1日开始
            end_date = None  # 默认到今天

            if len(sys.argv) > 2:
                # 检查是否指定了数据类型
                if sys.argv[2] not in ['20050101', '20050104']:  # 检查是否是日期格式
                    data_types = sys.argv[2].split(',')

                    # 如果指定了日期范围
                    if len(sys.argv) > 3:
                        start_date = sys.argv[3]
                        if len(sys.argv) > 4:
                            end_date = sys.argv[4]
                else:
                    data_types = None
                    start_date = sys.argv[2]
                    if len(sys.argv) > 3:
                        end_date = sys.argv[3]
            else:
                data_types = None

            run_initial_build(data_types, start_date, end_date)
        elif mode == '--test':
            # 使用多线程运行并发测试
            run_test_concurrent()
        else:
            print(f"未知模式: {mode}")
            print("可用模式: daily_update, initial_build, --test")
            print("示例: python main.py --test")
            print("      python main.py initial_build")
            print("      python main.py initial_build daily,moneyflow 20050101 20231231")
    else:
        # 默认运行每日更新
        run_daily_update_concurrent()


if __name__ == "__main__":
    main()