import time
import logging
import random
from config import API_MAX_RETRIES

# API调用历史记录，用于限频控制（每个接口独立）
api_call_history = {}

def safe_api_call(pro, method_name, **params):
    """
    安全的API调用，包含重试机制和限频控制
    """
    # 获取API限制
    api_name = params.get('api_name', method_name)
    from config import API_LIMITS
    # 使用特定接口的限制，如果没有配置则使用api_name作为默认键值
    api_limit = API_LIMITS.get(api_name)
    if api_limit is None:
        # 如果没有在API_LIMITS中找到对应接口的限制，尝试使用method_name
        api_limit = API_LIMITS.get(method_name, float('inf'))

    # 确保没有api_name参数传递给实际的API调用
    if 'api_name' in params:
        del params['api_name']

    # 重试机制
    for attempt in range(API_MAX_RETRIES):
        try:
            # 限频控制
            _rate_limit(api_name, api_limit)

            # 执行API调用
            method = getattr(pro, method_name)
            result = method(**params)

            return result
        except Exception as e:
            logging.warning(f"API调用失败 (第{attempt+1}次尝试): {api_name}, 错误: {str(e)}")

            if attempt < API_MAX_RETRIES - 1:
                # 指数退避策略
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"等待 {wait_time:.2f} 秒后重试...")
                time.sleep(wait_time)
            else:
                # 所有重试都失败了
                logging.error(f"API调用最终失败: {api_name}, 参数: {params}")
                raise e

def _rate_limit(api_name, api_limit):
    """
    限频控制 - 每个接口独立管理自己的调用频率
    """
    # 如果接口限制为无穷大，则不限频
    if api_limit == float('inf'):
        return

    current_time = time.time()

    # 初始化API调用历史（每个接口独立）
    if api_name not in api_call_history:
        api_call_history[api_name] = []

    # 清理一分钟前的调用记录
    api_call_history[api_name] = [
        call_time for call_time in api_call_history[api_name]
        if current_time - call_time < 60
    ]

    # 检查是否超过API限制
    if len(api_call_history[api_name]) >= api_limit:
        # 等待直到可以进行下一次调用
        sleep_time = 60 - (current_time - api_call_history[api_name][0])
        if sleep_time > 0:
            logging.info(f"{api_name} 接口达到限频，等待 {sleep_time:.2f} 秒")
            time.sleep(sleep_time)
            current_time = time.time()

    # 记录当前调用
    api_call_history[api_name].append(current_time)

def get_api_status():
    """
    获取API调用状态
    """
    current_time = time.time()
    status = {}
    
    for api_name, call_times in api_call_history.items():
        # 清理一分钟前的调用记录
        call_times = [t for t in call_times if current_time - t < 60]
        api_limit = API_LIMITS.get(api_name, API_RATE_LIMIT)
        
        status[api_name] = {
            'calls_in_last_minute': len(call_times),
            'limit': api_limit,
            'remaining_calls': max(0, api_limit - len(call_times)),
            'reset_in_seconds': 60 - (current_time - min(call_times)) if call_times else 0
        }
    
    return status

def reset_api_history():
    """
    重置API调用历史
    """
    global api_call_history
    api_call_history.clear()
    logging.info("API调用历史已重置")

def rate_limit_decorator(api_name, api_limit=None):
    """
    限频装饰器
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # 使用传入的限制或从配置中获取
            limit = api_limit or API_LIMITS.get(api_name, API_RATE_LIMIT)
            _rate_limit(api_name, limit)
            return func(*args, **kwargs)
        return wrapper
    return decorator