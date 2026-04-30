[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING.zh-CN.md)

# Contributing to HEPH

Thanks for contributing.

This public repository is the public codebase for HEPH, so contributions should stay within the repository boundary.

## Before you open an issue

- Check whether the issue belongs to the public client, desktop shell, frontend UI, or non-secret local workflow.
- Do not post secrets, tokens, private logs, or production configuration.
- If a problem depends on private infrastructure, describe the behavior without exposing internal credentials or private endpoints.

## Good contribution targets

- UI improvements
- desktop packaging improvements
- installer UX improvements
- documentation
- local developer experience
- non-secret miner/client behavior
- bug fixes that do not require private infrastructure access

## Out of scope for the public repo

Please do not open pull requests that require:

- production credentials
- private deployment access
- billing secrets
- admin-only control-plane logic
- unpublished internal operational data

## Development guidelines

- Keep paths portable and relative.
- Do not hardcode personal machine paths.
- Do not commit `.env` files, build output, installers, logs, caches, or bundled runtimes.
- Treat anything shipped to the client as inspectable.

## Pull request checklist

Before opening a PR, make sure:

1. the change stays inside the public-safe repo boundary
2. no secrets or machine-specific values were introduced
3. docs were updated if behavior changed
4. the app still works in the intended local workflow

## Security reports

If you find a security issue that could affect private infrastructure or user data, do not post the full exploit publicly in an issue. Report it privately to the project owner first.
