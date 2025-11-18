"""
内存监控和管理模块
用于监控内存使用情况、提供内存警告和控制内存使用
"""
import psutil
import logging
import time
from threading import Thread, Event
from typing import Callable, Optional
from config import get_memory_usage, get_dynamic_streaming_threshold


class MemoryMonitor:
    """
    内存监控器
    提供内存使用监控、警告和阈值控制功能
    """

    def __init__(self, warning_threshold: float = 75.0, critical_threshold: float = 85.0,
                 check_interval: float = 30.0, alert_callback: Optional[Callable] = None):
        """
        初始化内存监控器

        Args:
            warning_threshold: 警告阈值百分比
            critical_threshold: 严重阈值百分比
            check_interval: 检查间隔（秒）
            alert_callback: 警告回调函数
        """
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.check_interval = check_interval
        self.alert_callback = alert_callback
        self._stop_event = Event()
        self._monitor_thread = None
        self._is_monitoring = False

    def start_monitoring(self):
        """启动内存监控"""
        if not self._is_monitoring:
            self._stop_event.clear()
            self._monitor_thread = Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()
            self._is_monitoring = True
            logging.info(f"内存监控已启动，检查间隔: {self.check_interval}秒")

    def stop_monitoring(self):
        """停止内存监控"""
        if self._is_monitoring:
            self._stop_event.set()
            if self._monitor_thread:
                self._monitor_thread.join()
            self._is_monitoring = False
            logging.info("内存监控已停止")

    def _monitor_loop(self):
        """监控循环"""
        while not self._stop_event.is_set():
            try:
                memory_percent = get_memory_usage()

                # 检查是否需要发出警告
                if memory_percent >= self.critical_threshold:
                    self._trigger_alert('critical', memory_percent)
                elif memory_percent >= self.warning_threshold:
                    self._trigger_alert('warning', memory_percent)

                # 等待下一个检查周期
                if self._stop_event.wait(timeout=self.check_interval):
                    break  # 收到停止信号，退出循环
            except Exception as e:
                logging.error(f"内存监控出错: {str(e)}")
                # 等待一段时间后继续
                if self._stop_event.wait(timeout=5):
                    break

    def _trigger_alert(self, level: str, memory_percent: float):
        """触发警告"""
        message = f"内存使用率过高: {memory_percent:.1f}% (阈值: {self.critical_threshold if level == 'critical' else self.warning_threshold}%)"

        if level == 'warning':
            logging.warning(message)
        else:  # critical
            logging.error(message)

        # 调用自定义回调
        if self.alert_callback:
            try:
                self.alert_callback(level, memory_percent, message)
            except Exception as e:
                logging.error(f"执行内存警告回调时出错: {str(e)}")

    def get_current_usage(self) -> float:
        """获取当前内存使用率"""
        return get_memory_usage()

    def get_dynamic_threshold(self) -> int:
        """获取动态流式处理阈值"""
        return get_dynamic_streaming_threshold()

    def is_memory_pressure_high(self) -> bool:
        """检查内存压力是否高"""
        return self.get_current_usage() > self.warning_threshold

    def is_memory_pressure_critical(self) -> bool:
        """检查内存压力是否严重"""
        return self.get_current_usage() > self.critical_threshold


class MemoryAlertManager:
    """内存警告管理器"""

    def __init__(self):
        self.alerts = []
        self._last_alert_time = 0
        self._alert_cooldown = 300  # 5分钟冷却时间

    def should_send_alert(self) -> bool:
        """检查是否应该发送警告（避免过于频繁）"""
        current_time = time.time()
        if current_time - self._last_alert_time >= self._alert_cooldown:
            self._last_alert_time = current_time
            return True
        return False

    def add_alert(self, level: str, memory_percent: float, message: str):
        """添加警告记录"""
        alert = {
            'timestamp': time.time(),
            'level': level,
            'memory_percent': memory_percent,
            'message': message
        }
        self.alerts.append(alert)

        # 限制警告记录数量
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-50:]  # 保留最近50条

    def get_recent_alerts(self, count: int = 10) -> list:
        """获取最近的警告"""
        return self.alerts[-count:]


# 全局内存监控器实例
memory_monitor = MemoryMonitor()
memory_alert_manager = MemoryAlertManager()


def init_memory_monitor():
    """初始化内存监控"""
    memory_monitor.start_monitoring()


def shutdown_memory_monitor():
    """关闭内存监控"""
    memory_monitor.stop_monitoring()


def memory_safe_operation(operation: Callable, *args, **kwargs):
    """
    在内存安全的条件下执行操作
    监控内存使用并在内存紧张时采取适当措施
    """
    current_memory = memory_monitor.get_current_usage()

    # 如果内存使用率很高，先等待一段时间
    if memory_monitor.is_memory_pressure_critical():
        logging.warning(f"内存使用率过高 ({current_memory}%)，等待10秒再执行操作...")
        time.sleep(10)
        current_memory = memory_monitor.get_current_usage()

    if memory_monitor.is_memory_pressure_high():
        logging.info(f"内存使用率较高 ({current_memory}%)，谨慎执行操作...")

    # 执行操作
    result = operation(*args, **kwargs)

    # 再次检查内存使用情况
    final_memory = memory_monitor.get_current_usage()
    if final_memory > current_memory + 5:  # 内存增加了5%以上
        logging.warning(f"操作导致内存使用率增加: {current_memory:.1f}% -> {final_memory:.1f}%")

    return result


# 在模块加载时初始化内存监控
init_memory_monitor()


if __name__ == "__main__":
    # 测试内存监控
    print("内存使用率:", memory_monitor.get_current_usage())
    print("动态阈值:", memory_monitor.get_dynamic_threshold())
    print("是否内存压力高:", memory_monitor.is_memory_pressure_high())
    print("是否内存压力严重:", memory_monitor.is_memory_pressure_critical())