#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试get_file_base64函数的示例
"""

import os
import tempfile
from zampie_utils.request_utils import get_file_base64

def test_local_file():
    """测试本地文件功能"""
    # 创建一个临时文件
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("这是一个测试文件内容")
        temp_file = f.name
    
    try:
        # 测试本地文件
        base64_content = get_file_base64(temp_file)
        print(f"本地文件base64编码成功: {base64_content[:50]}...")
        return True
    except Exception as e:
        print(f"本地文件测试失败: {e}")
        return False
    finally:
        # 清理临时文件
        if os.path.exists(temp_file):
            os.unlink(temp_file)

def test_url():
    """测试URL功能（需要网络连接）"""
    try:
        # 使用一个小的测试图片URL
        test_url = "https://httpbin.org/base64/SFRUUEJJTiBpcyBhd2Vzb21l"
        base64_content = get_file_base64(test_url)
        print(f"URL文件base64编码成功: {base64_content[:50]}...")
        return True
    except Exception as e:
        print(f"URL测试失败: {e}")
        return False

def test_invalid_path():
    """测试无效路径"""
    try:
        get_file_base64("不存在的文件.txt")
        print("无效路径测试失败：应该抛出异常")
        return False
    except (FileNotFoundError, ValueError) as e:
        print(f"无效路径测试成功，正确抛出异常: {e}")
        return True
    except Exception as e:
        print(f"无效路径测试失败，抛出意外异常: {e}")
        return False

if __name__ == "__main__":
    print("开始测试get_file_base64函数...")
    
    # 测试本地文件
    print("\n1. 测试本地文件:")
    test_local_file()
    
    # 测试URL
    print("\n2. 测试URL:")
    test_url()
    
    # 测试无效路径
    print("\n3. 测试无效路径:")
    test_invalid_path()
    
    print("\n测试完成！")
