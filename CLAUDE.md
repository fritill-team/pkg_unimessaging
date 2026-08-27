# CLAUDE.md

This repository is a subrepo of an estate. **Nothing about how to work here lives in this
file** — it is a pointer, and by estate rule it stays one. If you opened a session in this
directory rather than at the estate root, read the estate first.

## Find the estate root

Walk **up** from here to the nearest ancestor containing `.estate-root`. Do not compute it
from a fixed depth and do not use `git rev-parse --show-toplevel` — from a worktree that
answers with the worktree, which is the defect `.agent/workspace.py` was written to fix.

## Read these, in this order

| # | File (at the estate root) | What it gives you |
|---|---|---|
| 1 | `AGENTS.md` | How to work in this estate. §1 is the routing rule; start there. |
| 2 | `.agent/INDEX.md` | Every ruled decision, one line each, and which file holds it. |
| 2 | `ARCHITECTURE-SPINE.md` | The estate spine. Its `AD`s bind every repo, including this one. |
| 3 | `.agent/keys/<ns>.md` | The distilled summary for one key — usually enough to act on. |
| 4 | the source the key names | Only when the summary is not enough. |

Cite keys namespaced (`estate:AD-7`), never bare — a bare `AD-N` is ambiguous across spines.

The KB answers **why**. For **where is X / what calls X / what breaks if I change X**, use the
code graph — it is derived from the AST, so it cannot be out of date with the code.

## The code graph: never pass `--global` from this estate

This estate uses one merged graph file of its own:

```bash
graphify query "..." --graph <estate-root>/.graphify/estate-graph.json
```

`--global` writes `~/.graphify/global-graph.json`, which belongs to a **different estate on
this machine**. `global_add` resolves a tag collision by warning to stderr and then
overwriting, mid-extract, with the warning scrolling past. `.agent/kb/graph-guard.sh` refuses
`--global` for this reason. The estate graph may not be built yet; if it is missing, query the
repo's own `graphify-out/graph.json` instead.

## What does not belong in this file

Project summaries, architecture narration, tech-stack lists, directory tours, lifecycle
models, memory-tool protocols. All of it goes stale in place and none of it is citable. A
durable decision belongs in the estate spine as an `AD`; everything else belongs in the code.
