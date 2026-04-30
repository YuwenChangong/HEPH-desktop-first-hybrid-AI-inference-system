[English](README.md) | [简体中文](README.zh-CN.md)

# HEPH（Hephaestus）

HEPH 是一个桌面优先的混合式 AI 推理系统，核心思路很简单：能本地跑就本地跑，适合远程跑就远程路由。

它把三种执行路径统一在同一个产品里：

- `Local`：设备本地推理
- `Remote`：由工作节点（miner）执行的远程推理
- `Auto`：根据本地能力在两者之间自动选择

最终体验是：本地能完成时保持轻量直接，需要扩展算力时再接入远程执行网络。

## 为什么是 HEPH

很多 AI 产品会强迫你在“纯本地”和“纯云端”之间二选一。

HEPH 选择了另一种方式：

- 把桌面应用作为第一入口
- 把本地算力当成一等执行路径
- 让远程工作节点扩展容量，而不是取代客户端
- 让路由、任务状态和工作节点（miner）流程在代码层保持可理解

如果你关注桌面 AI 产品、混合推理，或者分布式任务执行，这个仓库展示了这套系统的核心形态。

## 核心概念

### Local

通过本地模型和运行时直接在用户机器上执行。

### Remote

把任务排入队列，由工作节点（miner）执行并通过 gateway 完成结算。

### Auto

根据本地能力、模型可用性和执行约束，在本地与远程之间自动选择。

## 仓库结构

- `desktop/`
  - Electron 桌面壳、启动流程、打包和安装逻辑
- `frontend/`
  - 聊天 UI 与客户端逻辑
- `gateway-api/`
  - 路由、任务状态与计费相关流程的网关逻辑
- `miner/`
  - 工作节点（miner）侧接单与提交逻辑

## 这个仓库适合做什么

你可以用这个仓库来：

- 理解桌面端架构
- 查看客户端与工作节点的协作流程
- 了解 `local / auto / remote` 三种执行路径
- 提交产品、交互或实现层面的改进
- 研究桌面优先的分布式 AI 推理模式

## 开发说明

这个仓库的目标是可读、可公开、可移植。

部分与特定环境或部署相关的内容没有放进公开目录，所以更适合把它看作 HEPH 的公开核心，而不是完整私有工作区的镜像。

贡献规则和发布边界说明见 [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md)、[PUBLIC_REPO_GUIDE.zh-CN.md](PUBLIC_REPO_GUIDE.zh-CN.md) 和 [SECURITY.zh-CN.md](SECURITY.zh-CN.md)。

## License

本仓库采用 MIT License，详见 [LICENSE](LICENSE)。
