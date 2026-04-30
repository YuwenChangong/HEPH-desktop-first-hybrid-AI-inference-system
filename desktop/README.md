[English](README.md) | [简体中文](README.zh-CN.md)

# Desktop Packaging (Electron)

This directory contains the Electron shell and packaging workflow for the HEPH desktop app.

It wraps the existing local application stack without changing product behavior.

This public export keeps the Electron shell and packaging code for reference, but some private/runtime-only inputs from the full workspace are intentionally excluded.

## Run the desktop app in development

From the repository root:

```powershell
cd desktop
npm install
npm start
```

At startup, the desktop app will:

- start or restart the local runtime services
- wait for the local gateway health check
- open the chat interface inside Electron

On exit, it stops the local services it started.

## Scope note

Use this directory to review the desktop shell and packaging logic.

Do not assume this public export contains every private dependency needed to reproduce the exact internal production installer.

## Build the Windows installer

From the repository root:

```powershell
cd desktop
npm install
npm run dist:win
```

The generated installer is written to:

```text
desktop/dist/
```
