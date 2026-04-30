[English](README.md) | [简体中文](README.zh-CN.md)

# 工作节点（miner）

这个目录包含 HEPH 的工作节点执行流程。

代码里沿用 `miner` 这个词，但更准确的理解是“工作节点”。它负责：

- 从 gateway 拉取符合条件的远程任务
- 使用本地模型执行任务
- 把结果提交回网络

## 系统要求

- Python 3.10+
- 已安装并运行 Ollama
- Windows / macOS / Linux

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境

通过环境变量或本地配置提供 gateway 地址和访问令牌。

典型配置如下：

```text
GATEWAY_URL=https://your-gateway
ACCESS_TOKEN=your-access-token
```

### 3. 下载模型

根据你的显存选择合适的模型。

例如：

```bash
ollama pull qwen3.5:2b
ollama pull qwen3.5:9b
ollama pull qwen3.5:27b
```

### 4. 运行工作节点

```bash
python heph.py
```

## 自定义模型

你也可以让工作节点使用自定义 Ollama 模型，包括：

- 从 Ollama 官方库拉取的模型
- 通过 Ollama 支持的 Hugging Face 模型
- 本地导入的 GGUF 模型

## 常见环境变量

- `GATEWAY_URL`
  - gateway 地址
- `ACCESS_TOKEN`
  - miner 访问令牌
- `MINER_NAME`
  - 可选的固定 miner 名称
- `FORCE_VRAM`
  - 可选的显存手动覆盖值
- `CUSTOM_MODEL`
  - 可选的指定模型名
- `TARGET_MODE`
  - 可选的任务模式过滤

## 说明

这个公开仓库主要用于展示工作节点（miner）的工作流程和代码路径。

项目专用的生产凭据和部署相关配置不包含在这里。
