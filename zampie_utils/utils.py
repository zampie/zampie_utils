import os
import re
import time
import json

import random
import threading
from functools import wraps
import pandas as pd

import chardet

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
