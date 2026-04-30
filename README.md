[English](README.md) | [简体中文](README.zh-CN.md)

# HEPH (Hephaestus)

HEPH is a desktop-first hybrid AI inference system built around a simple idea: run locally when you can, route remotely when you should.

It combines three execution paths in one product:

- `Local` for on-device inference
- `Remote` for worker-node-backed execution
- `Auto` for capability-aware routing between the two

The result is a chat and task experience that stays close to the machine when possible, but can expand into a distributed execution network when more capacity is needed.

## Why HEPH

Most AI products force a hard choice between local-first and cloud-first.

HEPH is built around a different model:

- keep the desktop app as the primary surface
- treat local compute as a first-class execution path
- let remote worker nodes extend capacity instead of replacing the client
- keep routing, task state, and worker-node flow understandable in code

If you are interested in desktop AI products, hybrid inference, or distributed task execution, this repository shows the core shape of that system in code.

## Core concepts

### Local

Run directly on the user's machine through the local model/runtime path.

### Remote

Queue work for a worker node (`miner`) and settle it through the gateway.

### Auto

Choose between local and remote depending on local capability, model availability, and execution constraints.

## Repository structure

- `desktop/`
  - Electron shell, startup flow, packaging, and installer logic
- `frontend/`
  - chat UI and client-side application logic
- `gateway-api/`
  - gateway logic for routing, task state, and billing-related flow
- `miner/`
  - worker-node (`miner`) task claim and submit logic

## What this repository is for

Use this repository to:

- inspect a desktop-first AI product architecture
- review the client and worker-node workflow
- understand the local / auto / remote routing model
- contribute product, UX, or implementation improvements
- study a practical hybrid inference design instead of a cloud-only stack

## Development notes

This repository is meant to be readable, portable, and easy to inspect.

It focuses on the public core of HEPH: desktop flow, routing logic, frontend behavior, and worker-node execution. If you are reading the code to understand the product shape, the important paths are all here.

For contribution rules and release boundaries, see [CONTRIBUTING.md](CONTRIBUTING.md), [PUBLIC_REPO_GUIDE.md](PUBLIC_REPO_GUIDE.md), and [SECURITY.md](SECURITY.md).

## License

This repository is released under the MIT License. See [LICENSE](LICENSE).
