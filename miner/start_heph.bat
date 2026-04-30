@echo off
chcp 65001
cls

:: 1. 切换到脚本所在目录（防止双击运行时路径错乱）
cd /d "%~dp0"

:: 2. 自动检查并安装依赖到全局环境
:: 务实做法：既然要“直接使用”，就在启动前强制静默安装一次 requirements.txt
echo [环境检查] 正在确保全局运行库已就绪...
python -m pip install -r requirements.txt --quiet

:: 3. 定义全局 Python 启动指令
set PYTHON_EXE=python

:loop
cls
echo ----------------------------------------------------
echo [全局模式] 矿工守护进程运行中...
echo 目标等级: %HARDWARE_LEVEL% (基于系统 Python)
echo ----------------------------------------------------

:: 4. 执行脚本
%PYTHON_EXE% heph.py all

echo.
echo ----------------------------------------------------
echo 警告：检测到程序退出（显存溢出/网络波动）
echo 5秒后尝试自动重启，按 Ctrl+C 停止...
echo ----------------------------------------------------
timeout /t 5
goto loop