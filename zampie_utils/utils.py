import os
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import random
import threading
from functools import wraps
import pandas as pd

import chardet
import natsort

from .logger import Logger

logger = Logger()


def makedirs(path):
    """"""
    if not os.path.isdir(path):
        os.makedirs(path)


def walk(path, types=None, max_depth=float('inf')):
    """"""
    depth = 0
    for root, dirs, filenames in os.walk(path):
        if depth >= max_depth:
            break
        depth += 1
        for filename in filenames:
            if types:
                type = filename.split('.')[-1]
                # type = os.path.splitext(filename)[1]
                # print(type)
                if type in types:
                    yield os.path.join(root, filename)
                    continue
            else:
                yield os.path.join(root, filename)


def walk_dirs(path, max_depth=float('inf')):
    """"""
    depth = 0
    yield path  # 返回根目录
    for root, dirs, filenames in os.walk(path):
        if depth >= max_depth:
            break
        depth += 1
        for dir in dirs:
            yield os.path.join(root, dir)


def read_file(file_path):
    """"""
    print(f"Reading file: {file_path}")

    with open(file_path, 'rb') as f:
        content = f.read()
        detected = chardet.detect(content)
        encoding = detected['encoding']
        print(f"Detected encoding: {encoding}")
        content = content.decode(encoding)

    return content


def timer(timer_unit="ms", message=""):
    """"""

    def timer(fun):
        """"""

        @wraps(fun)
        def wrapper(*args, **kwargs):
            """"""
            nonlocal message
            message = message if message else fun.__name__

            logger.info(message + ": start")
            time_str = time.time()

            res = fun(*args, **kwargs)

            time_cost = time.time() - time_str
            unit = timer_unit
            if unit == "ms":
                time_cost = time_cost * 1000
            elif unit == "min":
                time_cost = time_cost / 60
            elif unit == "h":
                time_cost = time_cost / 3600
            else:
                unit = "s"

            logger.info(message + " cost: %.3f " % time_cost + unit)
            return res

        return wrapper

    return timer


class ContextTimer:
    """"""

    def __init__(self, name="timer", unit="ms"):
        """"""
        self.name = name
        self.unit = unit
        # print(self.name + ": timer init")

    def __enter__(self):
        """"""
        logger.info(self.name + ": timer start")
        self.time_str = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """"""
        time_cost = time.time() - self.time_str

        if self.unit == "ms":
            time_cost = time_cost * 1000
        elif self.unit == "min":
            time_cost = time_cost / 60
        elif self.unit == "h":
            time_cost = time_cost / 3600
        else:
            self.unit = "s"

        logger.info(self.name + " cost: %.2f " % time_cost + self.unit)
        # 返回True会导致程序不能退出
        # return True


def find_emojis(response):
    """
        匹配 emoji
    """

    emoji_pattern = re.compile(
        r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F'
        r'\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F'
        r'\U0001FA70-\U0001FAFF\U00002640\U00002642]',  # 添加 U+2640 和 U+2642 范围
        flags=re.UNICODE)

    emojis = emoji_pattern.findall(response)
    return emojis


def find_expressings(response):
    """
        匹配 emoji
    """

    expression_pattern = re.compile(r'<.*?>')

    # 查找匹配项
    expressions = expression_pattern.findall(response)
    return expressions


def probability_trigger(prob=0.5):
    """
    Args:
        prob: float, probability of triggering
    """
    if random.random() < prob:
        logger.info(f"probability_trigger: True")
        return True
    logger.info(f"probability_trigger: False")
    return False


def remove_blank_lines(dialog):
    """
    Args:
        dialog: str, dialog content
    """
    dialog = dialog.strip()
    dialog_list = []
    for line in dialog.split("\n"):
        if not line:
            continue
        dialog_list.append(line)

    dialog = "\n".join(dialog_list)

    return dialog


def save_jsonl(dict_list, file_name):
    '''
    保存dict_list为jsonl文件
    '''
    with open(file_name, 'w') as f:
        for d in dict_list:
            f.write(json.dumps(d, ensure_ascii=False) + '\n')


def save_json(obj, file_name):
    '''
    保存dict为json文件
    '''
    json.dump(obj, open(file_name, 'w'), ensure_ascii=False, indent=4)


def load_jsonl(file_name):
    '''
    读取jsonl文件，返回一个每行都解析过的list
    '''
    with open(file_name, 'r') as f:
        return [json.loads(line) for line in f]


def load_json(file_name):
    '''
    读取json文
    '''
    with open(file_name, 'r') as f:
        return json.load(f)


def retry(retries=10, delay=10):
    """
    description:
    param:
    return:
    """

    def decorator(func):
        """
        description:
        param:
        return:
        """

        def wrapper(*args, **kwargs):
            """
            description:
            param:
            return:
            """

            attempts = retries
            while attempts > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts -= 1
                    if attempts == 0:
                        print(f"retry failed")
                        raise Exception("retry failed")
                        # return None

                    # 给delay加上25%的随机数，避免多个请求同时重试
                    r_delay = delay + delay * 0.25 * (1 - 2 * random.random())
                    r_delay = max(0, r_delay)

                    print(
                        f"{e}, retrying after {r_delay:.2f} seconds, {attempts} retries left"
                    )

                    time.sleep(r_delay)

        return wrapper

    return decorator


def return_on_error(default_value=None):
    """
    装饰器：当函数执行失败时返回指定的默认值
    
    Args:
        default_value: 发生异常时返回的默认值，默认为 None
    
    Example:
        @return_on_error(default_value=[])
        def process_data():
            # 如果发生异常，将返回 []
    """

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"Function {func.__name__} failed with error: {str(e)}")
                return default_value

        return wrapper

    return decorator


def timer(timer_unit="ms", message=""):
    """"""

    def timer(fun):
        """"""

        @wraps(fun)
        def wrapper(*args, **kwargs):
            """"""
            nonlocal message
            message = message if message else fun.__name__

            print(message + ": start")
            time_str = time.time()

            res = fun(*args, **kwargs)

            time_cost = time.time() - time_str
            unit = timer_unit
            if unit == "ms":
                time_cost = time_cost * 1000
            elif unit == "min":
                time_cost = time_cost / 60
            elif unit == "h":
                time_cost = time_cost / 3600
            else:
                unit = "s"

            print(message + " cost: %.3f " % time_cost + unit)

            return res

        return wrapper

    return timer


def retry(retries=10, delay=10):
    """
    description:
    param:
    return:
    """

    def decorator(func):
        """
        description:
        param:
        return:
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            description:
            param:
            return:
            """

            attempts = retries
            while attempts > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts -= 1
                    if attempts == 0:
                        print(f"retry failed")
                        raise Exception("retry failed")
                        # return None

                    # 给delay加上25%的随机数，避免多个请求同时重试
                    r_delay = delay + delay * 0.25 * (1 - 2 * random.random())
                    r_delay = max(0, r_delay)

                    print(
                        f"{e}, retrying after {r_delay:.2f} seconds, {attempts} retries left"
                    )

                    time.sleep(r_delay)

        return wrapper

    return decorator


def sample_lines(lines, num, seed=None):
    if seed is not None:
        random.seed(seed)
    # 创建列表副本以避免修改原始列表
    lines_copy = lines.copy()
    # 对副本进行随机打乱
    random.shuffle(lines_copy)
    return lines_copy[:num]


def sample_files(files, num, seed=None):
    files = natsort.natsorted(files)
    files = sample_lines(files, num, seed)
    files = natsort.natsorted(files)
    return files


def parallel_map(func, items, max_workers=5):
    """
    并行执行函数，保证输出顺序与输入顺序一致
    
    Args:
        func: 要执行的函数
        items: 输入数据列表
        max_workers: 最大工作线程数，默认为 None（使用系统 CPU 核心数）
    
    Returns:
        按输入顺序排列的结果列表
    """
    results = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务，并保存任务和索引的映射
        future_to_index = {
            executor.submit(func, item): i
            for i, item in enumerate(items)
        }

        # 按完成顺序获取结果，但保存到正确的位置
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results[index] = future.result()
            except Exception as e:
                results[index] = e

    return results


def run_cmd(cmd, shell=True, timeout=None):
    """
    执行命令并返回结果
    
    Args:
        cmd: 要执行的命令
        shell: 是否使用shell执行，默认为True
        timeout: 超时时间（秒），默认为None（不超时）
    
    Returns:
        tuple: (return_code, stdout, stderr)
            - return_code: 命令的返回码，0表示成功
            - stdout: 标准输出
            - stderr: 标准错误
    
    Example:
        code, out, err = run_cmd("ls -l")
        if code == 0:
            print("命令执行成功：", out)
        else:
            print("命令执行失败：", err)
    """
    import subprocess

    try:
        process = subprocess.Popen(cmd,
                                   shell=shell,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)

        stdout, stderr = process.communicate(timeout=timeout)
        return_code = process.returncode

        return return_code, stdout.strip(), stderr.strip()

    except subprocess.TimeoutExpired:
        process.kill()
        return -1, "", f"命令执行超时（{timeout}秒）"
    except Exception as e:
        return -1, "", f"执行命令时发生错误：{str(e)}"
