import os
from setuptools import setup, find_packages

# 读取README.md文件
long_description = ""
if os.path.exists("README.md"):
    with open("README.md", "r", encoding="utf-8") as f:
        long_description = f.read()

setup(
    name="zampie_utils",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "chardet",
        "colorlog",
    ],
    author="zampie",
    author_email="zampielzp@gmail.com",
    description="A collection of utility functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="utils, tools, utilities",
    url="https://github.com/zampie/zampie_utils",
    project_urls={
        "Bug Reports": "https://github.com/zampie/zampie_utils/issues",
        "Source": "https://github.com/zampie/zampie_utils",
    },
)


# pip install -e .  # 安装开发模式
# pip install build twine  # 安装打包和上传工具
# python -m build  # 打包
# twine upload dist/*  # 上传
# pip install zampie_utils  # 安装