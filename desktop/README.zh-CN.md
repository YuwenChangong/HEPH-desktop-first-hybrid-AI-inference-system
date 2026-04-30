[English](README.md) | [简体中文](README.zh-CN.md)

# 桌面打包（Electron）

这个目录包含 HEPH 桌面应用的 Electron 壳层和打包流程。

它负责把现有本地应用栈封装成桌面端体验，而不改变核心产品行为。

这个公开仓库保留了 Electron 壳层和打包逻辑，但完整私有工作区中的某些运行时输入并未放入这里。

## 开发模式运行桌面应用

在仓库根目录执行：

```powershell
cd desktop
npm install
npm start
```

启动时桌面应用会：

- 启动或重启本地运行时服务
- 等待本地 gateway 健康检查通过
- 在 Electron 中打开聊天界面

退出时，它会停止自己启动的本地服务。

## 作用边界

这个目录适合用于查看桌面壳和打包逻辑。

不要默认它已经包含复现私有生产安装器所需的全部输入。

## 构建 Windows 安装包

在仓库根目录执行：

```powershell
cd desktop
npm install
npm run dist:win
```

生成的安装包输出到：

```text
desktop/dist/
```
