from pathlib import Path
from typing import Union

import pandas as pd

from .logger import logger
from .utils import makedirs


def read_df(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """
    根据文件后缀名自动使用pandas读取文件为DataFrame
    
    支持的文件格式：
    - CSV: .csv, .tsv, .txt
    - Excel: .xlsx, .xls, .xlsm, .xlsb
    - JSON: .json, .jsonl
    - Parquet: .parquet
    - Pickle: .pkl, .pickle
    - HTML: .html, .htm
    - XML: .xml
    - Feather: .feather
    - HDF5: .h5, .hdf5
    
    Args:
        file_path: 文件路径
        **kwargs: 传递给对应pandas读取函数的额外参数
        
    Returns:
        pd.DataFrame: 读取的DataFrame
        
    Raises:
        ValueError: 不支持的文件格式
        FileNotFoundError: 文件不存在
        
    Examples:
        >>> df = read_df('data.csv')
        >>> df = read_df('data.xlsx', sheet_name=0)
        >>> df = read_df('data.json', orient='records')
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    suffix = file_path.suffix.lower()
    
    # CSV格式
    if suffix in ['.csv']:
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_csv(file_path, **kwargs)
    
    # TSV格式（制表符分隔）
    if suffix in ['.tsv']:
        kwargs.setdefault('sep', '\t')
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_csv(file_path, **kwargs)
    
    # TXT格式（尝试作为CSV读取）
    if suffix in ['.txt']:
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_csv(file_path, **kwargs)
    
    # Excel格式
    if suffix in ['.xlsx', '.xls', '.xlsm', '.xlsb']:
        return pd.read_excel(file_path, **kwargs)
    
    # JSON格式
    if suffix in ['.json']:
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_json(file_path, **kwargs)
    
    # JSONL格式（每行一个JSON对象）
    if suffix in ['.jsonl']:
        kwargs.setdefault('lines', True)
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_json(file_path, **kwargs)
    
    # Parquet格式
    if suffix in ['.parquet']:
        return pd.read_parquet(file_path, **kwargs)
    
    # Pickle格式
    if suffix in ['.pkl', '.pickle']:
        return pd.read_pickle(file_path, **kwargs)
    
    # HTML格式（返回第一个表格）
    if suffix in ['.html', '.htm']:
        kwargs.setdefault('encoding', 'utf-8')
        dfs = pd.read_html(file_path, **kwargs)
        if dfs:
            logger.info(f"HTML文件包含 {len(dfs)} 个表格，返回第一个表格")
            return dfs[0]
        else:
            raise ValueError(f"HTML文件中未找到表格: {file_path}")
    
    # XML格式
    if suffix in ['.xml']:
        kwargs.setdefault('encoding', 'utf-8')
        return pd.read_xml(file_path, **kwargs)
    
    # Feather格式
    if suffix in ['.feather']:
        return pd.read_feather(file_path, **kwargs)
    
    # HDF5格式
    if suffix in ['.h5', '.hdf5']:
        if 'key' not in kwargs:
            raise ValueError("读取HDF5文件需要指定'key'参数，例如: read_df('data.h5', key='df')")
        return pd.read_hdf(file_path, **kwargs)
    
    # 不支持的文件格式
    raise ValueError(
        f"不支持的文件格式: {suffix}\n"
        f"支持格式: .csv, .tsv, .txt, .xlsx, .xls, .xlsm, .xlsb, "
        f".json, .jsonl, .parquet, .pkl, .pickle, .html, .htm, "
        f".xml, .feather, .h5, .hdf5"
    )


def save_df(df: pd.DataFrame, save_path: Union[str, Path], **kwargs) -> None:
    """
    根据文件后缀名自动使用pandas保存DataFrame到文件
    
    支持的文件格式：
    - CSV: .csv, .tsv, .txt
    - Excel: .xlsx, .xls, .xlsm, .xlsb
    - JSON: .json, .jsonl
    - Parquet: .parquet
    - Pickle: .pkl, .pickle
    - HTML: .html, .htm
    - XML: .xml
    - Feather: .feather
    - HDF5: .h5, .hdf5
    
    Args:
        df: 要保存的DataFrame
        save_path: 保存路径
        **kwargs: 传递给对应pandas保存函数的额外参数
        
    Raises:
        ValueError: 不支持的文件格式
        
    Examples:
        >>> save_df(df, 'data.csv')
        >>> save_df(df, 'data.xlsx', sheet_name='Sheet1')
        >>> save_df(df, 'data.json', orient='records')
        >>> save_df(df, 'data.jsonl')
    """
    save_path = Path(save_path)
    
    # 确保目录存在
    if save_path.parent:
        makedirs(save_path.parent)
    
    # 获取绝对路径用于日志
    abs_path = save_path.resolve()
    suffix = save_path.suffix.lower()
    rows, cols = df.shape
    
    # CSV格式
    if suffix in ['.csv']:
        kwargs.setdefault('encoding', 'utf-8')
        df.to_csv(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到CSV文件: {abs_path}")
        return
    
    # TSV格式（制表符分隔）
    if suffix in ['.tsv']:
        kwargs.setdefault('sep', '\t')
        kwargs.setdefault('encoding', 'utf-8')
        df.to_csv(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到TSV文件: {abs_path}")
        return
    
    # TXT格式（作为CSV保存）
    if suffix in ['.txt']:
        kwargs.setdefault('encoding', 'utf-8')
        df.to_csv(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到TXT文件: {abs_path}")
        return
    
    # Excel格式
    if suffix in ['.xlsx', '.xls', '.xlsm', '.xlsb']:
        df.to_excel(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到Excel文件: {abs_path}")
        return
    
    # JSON格式
    if suffix in ['.json']:
        kwargs.setdefault('encoding', 'utf-8')
        df.to_json(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到JSON文件: {abs_path}")
        return
    
    # JSONL格式（每行一个JSON对象）
    if suffix in ['.jsonl']:
        kwargs.setdefault('lines', True)
        kwargs.setdefault('orient', 'records')
        kwargs.setdefault('encoding', 'utf-8')
        df.to_json(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到JSONL文件: {abs_path}")
        return
    
    # Parquet格式
    if suffix in ['.parquet']:
        df.to_parquet(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到Parquet文件: {abs_path}")
        return
    
    # Pickle格式
    if suffix in ['.pkl', '.pickle']:
        df.to_pickle(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到Pickle文件: {abs_path}")
        return
    
    # HTML格式
    if suffix in ['.html', '.htm']:
        kwargs.setdefault('encoding', 'utf-8')
        df.to_html(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到HTML文件: {abs_path}")
        return
    
    # XML格式
    if suffix in ['.xml']:
        kwargs.setdefault('encoding', 'utf-8')
        df.to_xml(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到XML文件: {abs_path}")
        return
    
    # Feather格式
    if suffix in ['.feather']:
        df.to_feather(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列) 到Feather文件: {abs_path}")
        return
    
    # HDF5格式
    if suffix in ['.h5', '.hdf5']:
        if 'key' not in kwargs:
            raise ValueError("保存HDF5文件需要指定'key'参数，例如: save_df(df, 'data.h5', key='df')")
        df.to_hdf(save_path, **kwargs)
        logger.info(f"成功保存DataFrame (形状: {rows}行 × {cols}列, key: {kwargs.get('key')}) 到HDF5文件: {abs_path}")
        return
    
    # 不支持的文件格式
    raise ValueError(
        f"不支持的文件格式: {suffix}\n"
        f"支持格式: .csv, .tsv, .txt, .xlsx, .xls, .xlsm, .xlsb, "
        f".json, .jsonl, .parquet, .pkl, .pickle, .html, .htm, "
        f".xml, .feather, .h5, .hdf5"
    )

