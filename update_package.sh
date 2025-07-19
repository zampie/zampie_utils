#!/bin/bash

# 自动更新zampie_utils包的脚本
set -e  # 遇到错误时退出

echo "🚀 开始自动更新zampie_utils包..."

# 检查是否在正确的目录
if [ ! -f "setup.py" ]; then
    echo "❌ 错误：未找到setup.py文件，请确保在项目根目录运行此脚本"
    exit 1
fi

# 清理之前的构建文件
echo "🧹 清理之前的构建文件..."
rm -rf dist/
rm -rf build/
rm -rf *.egg-info/

# 安装/更新构建工具
echo "📦 确保构建工具是最新的..."
pip3 install --upgrade build twine

# 构建包
echo "🔨 构建包..."
python3 -m build

# 检查构建是否成功
if [ ! -d "dist" ]; then
    echo "❌ 错误：构建失败，未生成dist目录"
    exit 1
fi

# 上传到PyPI
echo "📤 上传到PyPI..."
twine upload dist/*

echo "✅ 包更新完成！"
echo "📋 现在可以使用以下命令安装最新版本："
echo "   pip3 install --upgrade zampie_utils" 