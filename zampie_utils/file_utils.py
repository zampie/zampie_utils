import json
from typing import List, Dict, Any, Union
import chardet
from pathlib import Path
import uuid
from .logger import logger
from datetime import datetime


def save_jsonl(
    dict_list: List[Dict],
    file_path: Union[str, Path],
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
) -> None:
    """
    保存字典列表为JSONL文件

    Args:
        dict_list: 要保存的字典列表
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8
        ensure_ascii: JSON序列化时是否确保ASCII编码
    """
    file_path = Path(file_path)

    # 确保目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "w", encoding=encoding) as f:
            for d in dict_list:
                f.write(json.dumps(d, ensure_ascii=ensure_ascii) + "\n")
        logger.info(f"成功保存JSONL文件: {file_path}")
    except Exception as e:
        logger.error(f"保存JSONL文件失败: {file_path}, 错误: {e}")
        raise


def save_json(
    obj: Any,
    file_path: Union[str, Path],
    indent: int = 4,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
) -> None:
    """
    保存对象为JSON文件

    Args:
        obj: 要保存的对象
        file_path: 文件路径
        indent: 缩进空格数
        encoding: 编码格式，默认为utf-8
        ensure_ascii: JSON序列化时是否确保ASCII编码
    """
    file_path = Path(file_path)

    # 确保目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "w", encoding=encoding) as f:
            json.dump(obj, f, ensure_ascii=ensure_ascii, indent=indent)
        logger.info(f"成功保存JSON文件: {file_path}")
    except Exception as e:
        logger.error(f"保存JSON文件失败: {file_path}, 错误: {e}")
        raise


def load_jsonl(file_path: Union[str, Path], encoding: str = "utf-8") -> List[Dict]:
    """
    读取JSONL文件，返回解析后的字典列表

    Args:
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8

    Returns:
        解析后的字典列表
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        raise FileNotFoundError(f"文件不存在: {file_path}")

    try:
        with open(file_path, "r", encoding=encoding) as f:
            result = [json.loads(line.strip()) for line in f if line.strip()]
        logger.info(f"成功读取JSONL文件: {file_path}, 共{len(result)}行")
        return result
    except Exception as e:
        logger.error(f"读取JSONL文件失败: {file_path}, 错误: {e}")
        raise


def load_json(file_path: Union[str, Path], encoding: str = "utf-8") -> Any:
    """
    读取JSON文件

    Args:
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8

    Returns:
        解析后的对象
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        raise FileNotFoundError(f"文件不存在: {file_path}")

    try:
        with open(file_path, "r", encoding=encoding) as f:
            result = json.load(f)
        logger.info(f"成功读取JSON文件: {file_path}")
        return result
    except Exception as e:
        logger.error(f"读取JSON文件失败: {file_path}, 错误: {e}")
        raise


def load_text(
    file_path: Union[str, Path],
    encoding: str = "utf-8",
    auto_detect: bool = True,
) -> str:
    """
    读取文本文件，支持自动编码检测

    Args:
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8
        auto_detect: 是否自动检测编码

    Returns:
        文件内容
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        raise FileNotFoundError(f"文件不存在: {file_path}")

    try:
        if auto_detect:
            with open(file_path, "rb") as f:
                content = f.read()
                detected = chardet.detect(content)
                detected_encoding = detected["encoding"]
                logger.info(f"检测到编码: {detected_encoding}")
                content = content.decode(detected_encoding)
        else:
            with open(file_path, "r", encoding=encoding) as f:
                content = f.read()

        logger.info(f"成功读取文本文件: {file_path}")
        return content
    except Exception as e:
        logger.error(f"读取文本文件失败: {file_path}, 错误: {e}")
        raise


def write_text(
    content: str, file_path: Union[str, Path], encoding: str = "utf-8"
) -> None:
    """
    写入文本文件

    Args:
        content: 要写入的内容
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8
    """
    file_path = Path(file_path)

    # 确保目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "w", encoding=encoding) as f:
            f.write(content)
        logger.info(f"成功写入文本文件: {file_path}")
    except Exception as e:
        logger.error(f"写入文本文件失败: {file_path}, 错误: {e}")
        raise


def append_text(
    content: str, file_path: Union[str, Path], encoding: str = "utf-8"
) -> None:
    """
    追加文本到文件

    Args:
        content: 要追加的内容
        file_path: 文件路径
        encoding: 编码格式，默认为utf-8
    """
    file_path = Path(file_path)

    # 确保目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(file_path, "a", encoding=encoding) as f:
            f.write(content)
        logger.info(f"成功追加文本到文件: {file_path}")
    except Exception as e:
        logger.error(f"追加文本到文件失败: {file_path}, 错误: {e}")
        raise


def exists(file_path: Union[str, Path]) -> bool:
    """
    检查文件是否存在

    Args:
        file_path: 文件路径

    Returns:
        文件是否存在
    """
    return Path(file_path).exists()


def get_file_size(file_path: Union[str, Path]) -> int:
    """
    获取文件大小（字节）

    Args:
        file_path: 文件路径

    Returns:
        文件大小
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    return file_path.stat().st_size


def delete_file(file_path: Union[str, Path]) -> None:
    """
    删除文件

    Args:
        file_path: 文件路径
    """
    file_path = Path(file_path)
    if file_path.exists():
        file_path.unlink()
        logger.info(f"成功删除文件: {file_path}")
    else:
        logger.warning(f"文件不存在，无法删除: {file_path}")


# 为了保持向后兼容，提供别名函数
def read_file(file_path, encoding=None):
    """向后兼容的函数接口"""
    return load_text(file_path, encoding)


def gen_random_name(length=16):
    """生成随机文件名"""
    return str(uuid.uuid4())[:length]


def insert_text_before_ext(file_name, text, ext=None):
    """在文件名扩展名前插入文本"""
    path = Path(file_name)

    if ext is None:
        # 使用原始文件的扩展名
        new_name = f"{path.stem}{text}{path.suffix}"
    else:
        # 使用指定的扩展名
        new_name = f"{path.stem}{text}{ext}"

    return new_name


# 提取文件基础名称
def extract_base_name(file_name):
    """提取文件基础名称"""
    base_name = Path(file_name).name
    return base_name


# 提取文件扩展名
def extract_ext(file_name):
    """提取文件扩展名"""
    return Path(file_name).suffix


# 改变扩展名
def change_ext(file_name, ext):
    """改变扩展名"""
    return Path(file_name).stem + ext


def gen_timestamp_str(prefix="", suffix="", sep="_", format="%m%d%H"):
    """生成当前时间字符串"""
    now_str = datetime.now().strftime(format)

    if prefix:
        prefix = f"{prefix}{sep}"

    if suffix:
        suffix = f"{sep}{suffix}"

    name = f"{prefix}{now_str}{suffix}"

    return name


def gen_timestamp_file_name(
    prefix="", suffix="", ext="", sep="_", auto_rename=True, format="%m%d%H"
):
    """生成当前时间文件名"""
    now_str = datetime.now().strftime(format)

    prefix = str(prefix)

    if ext and not ext.startswith("."):
        ext = f".{ext}"

    if prefix and not prefix.endswith(("/", "\\")):
        prefix = f"{prefix}{sep}"

    if suffix:
        suffix = f"{sep}{suffix}"

    file_name = f"{prefix}{now_str}{suffix}{ext}"

    if auto_rename:
        file_name = rename_conflict_file(file_name, sep)

    logger.info(f"生成文件名: {file_name}")
    return file_name


def gen_file_name(name="", ext="", sep="_", auto_rename=True):
    """生成文件名"""
    name = str(name)

    if not name or name.endswith(("/", "\\")):
        name = gen_random_name()

    if ext and not ext.startswith("."):
        ext = f".{ext}"

    file_name = f"{name}{ext}"

    if auto_rename:
        file_name = rename_conflict_file(file_name, sep)

    logger.info(f"生成文件名: {file_name}")
    return file_name


def rename_conflict_file(file_name, sep="_"):
    """重命名冲突文件，添加数字后缀"""
    original_file_name = file_name
    prefix = str(Path(file_name).with_suffix(""))
    ext = Path(file_name).suffix

    count = 0
    while Path(file_name).exists():
        count += 1
        file_name = f"{prefix}{sep}{count:02d}{ext}"

        # 防止无限循环
        if count > 99999:
            logger.error(f"无法为文件 {original_file_name} 找到合适的名称")
            raise FileExistsError(f"无法为文件 {original_file_name} 找到合适的名称")

    # 只有在重命名后才记录成功信息
    if count > 0:
        logger.warning(f"文件 {original_file_name} 已存在，已重命名为: {file_name}")

    return file_name
