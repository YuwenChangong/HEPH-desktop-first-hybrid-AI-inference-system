[English](README.md) | [简体中文](README.zh-CN.md)

# Miner

This directory contains the worker-node workflow for HEPH.

A worker node, referred to in the codebase as a `miner`, is a local worker that:

- pulls eligible remote tasks from the gateway
- runs the selected model locally
- submits results back to the network

## Requirements

- Python 3.10+
- Ollama installed and running
- Windows, macOS, or Linux

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Provide your gateway URL and access token through environment variables or your local setup.

Typical values include:

```text
GATEWAY_URL=https://your-gateway
ACCESS_TOKEN=your-access-token
```

### 3. Pull a model

Choose a model that matches your available VRAM.

Examples:

```bash
ollama pull qwen3.5:2b
ollama pull qwen3.5:9b
ollama pull qwen3.5:27b
```

### 4. Run the worker node

```bash
python heph.py
```

## Custom models

You can also point the miner at a custom Ollama model, including:

- models pulled from the Ollama library
- Hugging Face models supported through Ollama
- locally imported GGUF-based models

## Common environment variables

- `GATEWAY_URL`
  - gateway endpoint
- `ACCESS_TOKEN`
  - miner access token
- `MINER_NAME`
  - optional fixed miner identity
- `FORCE_VRAM`
  - optional manual VRAM override
- `CUSTOM_MODEL`
  - optional explicit model name
- `TARGET_MODE`
  - optional task-mode filter

## Notes

This public repository documents the worker-node (`miner`) workflow and code path.

Project-specific production credentials and deployment-specific values are intentionally not included here.
