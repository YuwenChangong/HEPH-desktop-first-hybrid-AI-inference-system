[English](PUBLIC_REPO_GUIDE.md) | [简体中文](PUBLIC_REPO_GUIDE.zh-CN.md)

# HEPH Public Repo Guide

This repository is the public-facing core of HEPH (Hephaestus).

Use it as the GitHub-visible codebase for architecture review, discussion, public issues, and community contributions.

## Recommended use

This repository is a good place for:

- desktop shell improvements
- frontend UX changes
- worker-node workflow discussion
- gateway routing review
- documentation and developer experience fixes

## Working model

HEPH follows a desktop-first, hybrid inference model:

- local execution when the machine can handle the task
- remote execution when external capacity is the better path
- automatic routing between the two

That split is the core idea of the project, and this repository is organized around it.

## Publishing guidance

Publish this folder instead of a larger private working directory.

Before pushing a public update:

1. verify the repository still contains only source, docs, and intended public assets
2. verify there are no local logs, bundled caches, installers, or temporary build outputs
3. verify environment templates contain placeholders only
4. test the public-facing flow you want contributors to understand

## Contribution boundary

Good contribution targets include:

- desktop packaging and startup UX
- chat and task UI
- worker-node-side workflow quality
- routing visibility and developer ergonomics
- documentation

Some internal or deployment-specific pieces may exist outside this public tree by design, so contributions should stay inside the public repository boundary.

## Release mindset

Treat the public repository like a product-facing code surface:

- keep documentation portable
- avoid machine-specific assumptions
- prefer clean examples over internal shortcuts
- assume contributors will read the repository without your private workspace context

## License

HEPH is published here under the MIT License.
