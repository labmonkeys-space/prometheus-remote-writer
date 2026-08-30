# Repository conventions for AI agents

The canonical repository for this project is:

> **`labmonkeys-space/prometheus-remote-writer`**
> https://github.com/labmonkeys-space/prometheus-remote-writer

This file complements [`CLAUDE.md`](CLAUDE.md) (Claude-specific authoring conventions), [`RELEASING.md`](RELEASING.md) (release procedure), and [`CHANGELOG.md`](CHANGELOG.md). It does not duplicate their content; it adds one rule that applies before any of them: **where work lands**.

## Where work lands

All commits, branches, tags, and pull requests originating from automated agents MUST be made against the canonical repository. The legacy URL `labmonkeys-space/prometheus-remote-writer` is preserved by GitHub only as a redirect for human convenience; it is not a writable target.

Agents MUST NOT:

- Push commits, branches, or tags to `labmonkeys-space/prometheus-remote-writer`, `indigo423/prometheus-remote-writer`, or any other namespace.
- Open pull requests targeting any repository other than `labmonkeys-space/prometheus-remote-writer`.
- Honour a stale `origin` remote silently. If the local clone's `origin` points at a non-canonical URL (including the legacy `labmonkeys-space` URL preserved by GitHub's redirect), the agent MUST surface this to the human contributor and stop, rather than push through the redirect.

The canonical fix when a stale `origin` is observed:

```bash
git remote set-url origin https://github.com/labmonkeys-space/prometheus-remote-writer.git
```

## Contributors using a personal fork

Human contributors MAY use a personal GitHub fork to propose changes. When they do, the local clone's git remotes MUST follow the layout:

| Remote     | Points at                                  |
|------------|--------------------------------------------|
| `origin`   | the contributor's personal fork            |
| `upstream` | `labmonkeys-space/prometheus-remote-writer`   |

Pull requests opened from a personal fork MUST target `upstream/main`, never the contributor's own fork.

## Why this matters

The project is licensed Apache 2.0 and lives in labmonkeys-space, its canonical home. Pushes to personal namespaces fragment the project's history and confuse contributors looking for the canonical home. Keeping work centralised on `labmonkeys-space/prometheus-remote-writer` is what makes the rest of the workflow (releases, docs publish, issue tracking) coherent.
