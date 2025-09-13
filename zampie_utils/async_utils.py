from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress
from functools import wraps

from .logger import logger


def submit_task(func, item):
    """根据item的类型决定如何调用函数"""
    if isinstance(item, dict):
        # 检查是否是混合格式 {'args': (...), 'kwargs': {...}}
        if "args" in item and "kwargs" in item:
            return func(*item["args"], **item["kwargs"])
        elif "args" in item:
            return func(*item["args"])
        elif "kwargs" in item:
            return func(**item["kwargs"])
        else:
            # 纯字典，作为关键字参数
            return func(**item)
    elif isinstance(item, (tuple, list)) and not isinstance(item, str):
        # 元组或列表，作为位置参数
        return func(*item)
    else:
        # 单个值
        return func(item)


def sequential_map(func, items, description=None, log_level="none"):
    """
    顺序执行函数，用于单线程场景（如调试）

    Args:
        func: 要执行的函数
        items: 输入数据列表
        description: 进度条描述，默认为函数名 + "running"

    Returns:
        按输入顺序排列的结果列表
    """
    results = []

    # 检查是否支持len，如果不支持则转换为列表
    if not hasattr(items, "__len__"):
        logger.info("Converting iterator to list for map processing...")
        items = list(items)

    total = len(items)

    # 使用函数名 + "running" 作为默认描述
    if description is None:
        func_name = getattr(func, "__name__", "task")
        description = f"[green]<{func_name}>[/green] running"

    with Progress() as progress:
        total_task = progress.add_task(f"[cyan]{description}...[/cyan]", total=total)
        for i, item in enumerate(items):
            try:
                result = submit_task(func, item)
                logger.log(log_level, f"index: {i}, result: {result}")
                results.append(result)
            except Exception as e:
                logger.error(f"Error in sequential_map: {e}")
                results.append(e)
            progress.update(total_task, advance=1)
    return results


def parallel_map(func, items, description=None, log_level="none", max_workers=5):
    """
    并行执行函数，保证输出顺序与输入顺序一致

    Args:
        func: 要执行的函数
        items: 输入数据列表，支持以下格式：
            - 单个值的列表: [1, 2, 3]
            - 元组列表（位置参数）: [(1, 2), (3, 4), (5, 6)]
            - 字典列表（关键字参数）: [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
            - 混合格式：[{'args': (...), 'kwargs': {...}}, ...]
        description: 进度条描述，默认为函数名 + "running"
        log_level: 日志级别
        max_workers: 最大工作线程数，默认为 5

    Returns:
        按输入顺序排列的结果列表

    Examples:
        # 单个参数
        parallel_map(lambda x: x * 2, [1, 2, 3])

        # 多个位置参数
        parallel_map(lambda x, y: x + y, [(1, 2), (3, 4), (5, 6)])

        # 关键字参数
        parallel_map(lambda a, b: a + b, [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])

        # 混合参数
        parallel_map(lambda x, y, z=0: x + y + z,
                    [{'args': (1, 2), 'kwargs': {'z': 3}},
                     {'args': (4, 5), 'kwargs': {'z': 6}}])
    
    Problem:
        Process上下文中会导致icecream不可用,因为
        # icecream的ic()函数默认将输出写入stderr（标准错误流），而不是stdout（标准输出流）。这会导致：
        # 输出缓冲问题: stderr和stdout有不同的缓冲策略
        # 输出顺序混乱: 当使用map等函数时，多个ic()调用的输出可能交错
        需要手动配置输出函数为print
        ic.configureOutput(outputFunction=print)

    """
    # 如果只有一个worker或更少，使用顺序执行，避免线程开销
    if max_workers <= 1:
        return sequential_map(func, items, description, log_level)

    # 检查是否支持len，如果不支持则转换为列表
    if not hasattr(items, "__len__"):
        logger.info("Converting iterator to list for map processing...")
        items = list(items)

    # 使用函数名 + "running" 作为默认描述
    if description is None:
        func_name = getattr(func, "__name__", "task")
        description = f"[green]<{func_name}>[/green] running"

    results = [None] * len(items)

    with Progress() as progress:
        total_task = progress.add_task(f"[cyan]{description}...[/cyan]", total=len(items))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务，并保存任务和索引的映射
            future_to_index = {
                executor.submit(submit_task, func, item): i
                for i, item in enumerate(items)
            }

            # 按完成顺序获取结果，但保存到正确的位置
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    logger.log(log_level, f"index: {index}, result: {future.result()}")
                    results[index] = future.result()
                except Exception as e:
                    logger.error(f"Error in parallel_map: {e}")
                    results[index] = e
                progress.update(total_task, advance=1)

    return results


def auto_map(func, items, description=None, log_level="none", max_workers=1):
    """
    智能映射函数，根据max_workers自动选择执行方式

    Args:
        func: 要执行的函数
        items: 输入数据列表
        description: 进度条描述，默认为函数名 + "running"
        max_workers: 最大工作线程数，默认为 1（顺序执行）

    Returns:
        按输入顺序排列的结果列表
    """
    return parallel_map(func, items, description, log_level, max_workers)
