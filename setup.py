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
    author="liuzepeng01@baidu.com",
    author_email="liuzepeng01@baidu.com",
    description="A collection of utility functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
) 