# Obsidian Multi-Agent Hub Design

**Date:** 2026-04-13
**Vault:** `ideas` (`/home/nrodriguezm/Documents/ideas`)
**Status:** Implemented

## Purpose

Use the Obsidian `ideas` vault as a shared communication layer between AI agents
working on different openclaw projects. Agents read and write structured notes to
stay in sync with each other and with the user.

## Structure

```
ideas/
└── openclaw/
    ├── index.md              ← hub: project list + infrastructure table
    └── projects/
        └── <project>.md      ← one page per project
```

Existing vault content (`Curso Excel.md`, `Segundo Cerebro.md`) stays at the root,
untouched. All openclaw content lives under `openclaw/`.

## Conventions

1. **Frontmatter on every note** — required fields: `type`, `updated`, `updated_by`.
   Project pages add: `project`, `stack`, `repo`.

2. **Project pages are single source of truth** — purpose, stack, key paths,
   database references, jobs list, current status. Always at
   `openclaw/projects/<name>.md`.

3. **Status updates appended at the bottom** — reverse-chronological, dated, signed
   with agent identity (e.g. `rupe-agent@spider1`). Short factual entries.

4. **`index.md` hub** — map-of-content with wikilinks to every project page.
   Agents check this first to discover what exists.

5. **No credentials in the vault** — reference `.env` paths or project names only.

## Communication model

**Reference layer + lightweight status** (option A from brainstorming).

- Project pages hold stable reference content.
- Agents append dated status entries when they make meaningful changes.
- No explicit inter-agent messaging or lock/claim mechanism.
- If messaging is needed later, add an `openclaw/messages/` folder (upgrade to
  option B is one folder away).

## Access

Agents interact via the `obsidian` CLI at `/home/nrodriguezm/.local/bin/obsidian`
(drives the running desktop app, requires app to be running). Target vault with
`vault=ideas`. Key commands: `create`, `read`, `append`, `search`, `properties`,
`property:read`, `property:set`.

## Initial content

- `openclaw/index.md` — lists rupe, infrastructure table (spider1)
- `openclaw/projects/rupe.md` — full project page with stack, paths, 14 pipeline
  jobs, pending remote work note, initial status log entry
