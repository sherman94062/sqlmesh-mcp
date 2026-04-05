# SQLMesh MCP Server — Internal Architecture

## The Problem

SQLMesh's `Context` constructor initializes an **internal event dispatcher** (used for
scheduling, state tracking, and signal handling). When created inside the MCP server's
async event loop — even in a thread executor — it deadlocks. The two event systems
compete for control and neither can make progress.

## The Solution: Process Isolation

Every tool call spawns a **short-lived subprocess** that creates its own SQLMesh Context,
executes the operation, prints JSON to stdout, and exits. The MCP server process never
imports SQLMesh directly.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MCP CLIENT                                   │
│              (Claude Desktop / Claude Code / etc.)                   │
│                                                                     │
│  User: "What models are in my project?"                             │
│         │                                                           │
│         ▼                                                           │
│  Claude LLM decides to call: sqlmesh_model_list()                   │
└────────────────────────┬────────────────────────────────────────────┘
                         │  JSON-RPC over stdio
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MCP SERVER PROCESS                               │
│                  (python -m sqlmesh_mcp.server)                      │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              FastMCP  (asyncio event loop)                     │  │
│  │                                                               │  │
│  │  Handles:  JSON-RPC protocol                                  │  │
│  │            Tool registration & dispatch                       │  │
│  │            stdio transport (stdin/stdout)                     │  │
│  │                                                               │  │
│  │  ⚠ Does NOT import sqlmesh — no Context created here          │  │
│  └──────────────────────────┬────────────────────────────────────┘  │
│                              │                                      │
│                              │  await _run_python_script(...)       │
│                              │  (asyncio.create_subprocess_exec)    │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    SUBPROCESS  (one per tool call)             │  │
│  │                                                               │  │
│  │   1. import sqlmesh                                           │  │
│  │   2. Create Context(paths=[project_path])                     │  │
│  │      └─ Reads config.yaml                                    │  │
│  │      └─ Starts internal event dispatcher ← (the problem)     │  │
│  │      └─ Connects to database                                 │  │
│  │   3. Execute operation (list models, plan, query, etc.)       │  │
│  │   4. print(json.dumps(result)) → stdout                      │  │
│  │   5. Exit                                                     │  │
│  │                                                               │  │
│  └──────────┬──────────────────────────────────┬─────────────────┘  │
│             │                                  │                    │
└─────────────┼──────────────────────────────────┼────────────────────┘
              │                                  │
              ▼                                  ▼
┌──────────────────────────┐    ┌──────────────────────────────────────┐
│   SQLMesh Project Dir    │    │          PostgreSQL                   │
│  (e.g. ~/SQLMesh-AI)     │    │     (or other warehouse)             │
│                          │    │                                      │
│  config.yaml             │    │  ┌────────────────────────────────┐  │
│  models/                 │    │  │  sqlmesh_tpch database         │  │
│    staging/              │    │  │                                │  │
│    marts/                │    │  │  - State tables (snapshots,    │  │
│  seeds/                  │    │  │    environments, versions)     │  │
│  audits/                 │    │  │  - Model output tables         │  │
│                          │    │  │    (staging, marts)            │  │
└──────────────────────────┘    │  └────────────────────────────────┘  │
                                └──────────────────────────────────────┘
```

## Request Lifecycle

```
 Claude Desktop                MCP Server               Subprocess            PostgreSQL
      │                            │                        │                      │
      │  tools/call                │                        │                      │
      │  sqlmesh_model_list        │                        │                      │
      │ ──────────────────────►    │                        │                      │
      │                            │                        │                      │
      │                            │  python -c "..."       │                      │
      │                            │ ──────────────────►    │                      │
      │                            │                        │                      │
      │                            │                        │  Context(**kwargs)    │
      │                            │                        │ ───────────────────►  │
      │                            │                        │                      │
      │                            │                        │  ◄── state/config ──  │
      │                            │                        │                      │
      │                            │                        │  ctx.models          │
      │                            │                        │  (reads SQL files)   │
      │                            │                        │                      │
      │                            │  stdout: JSON result   │                      │
      │                            │ ◄──────────────────    │                      │
      │                            │                     (exits)                   │
      │  JSON-RPC response         │                                               │
      │ ◄──────────────────────    │                                               │
      │                            │                                               │
```

## Why Not Thread Executor?

We tried `asyncio.run_in_executor()` first. It failed because:

1. **SQLMesh Context** creates an internal event dispatcher on init
2. The dispatcher registers **signal handlers** (which must be on the main thread)
3. Inside a thread executor, `signal.signal()` raises `ValueError: signal only works in main thread`
4. Even when signal handling is worked around, the dispatcher's internal scheduling
   conflicts with the MCP server's asyncio loop, causing **deadlock**

The subprocess approach is the only clean solution — each SQLMesh operation gets its
own process with its own main thread and event loop, completely isolated from the
MCP server.

## Trade-offs

| Aspect | Subprocess Approach | In-Process (broken) |
|--------|-------------------|-------------------|
| **Reliability** | No deadlocks | Deadlocks on tool calls |
| **Latency** | ~1-2s overhead per call (Python startup + SQLMesh init) | Would be faster if it worked |
| **Memory** | Temporary spike per call, then freed | Persistent Context in memory |
| **Concurrency** | Multiple tools can run in parallel | Event loop blocked during execution |
| **Isolation** | Errors in SQLMesh can't crash the server | Exceptions could corrupt server state |

## Configuration

The server reads two environment variables:

- **`SQLMESH_PROJECT_PATH`** — path to any SQLMesh project directory containing a `config.yaml`
- **`SQLMESH_GATEWAY`** (optional) — named gateway from the project's config

These are passed through to each subprocess. The MCP server itself has no dependency
on which database engine or warehouse the SQLMesh project uses.
