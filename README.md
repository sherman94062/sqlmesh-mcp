# sqlmesh-mcp

> **MCP server for [SQLMesh](https://sqlmesh.com)** — exposes data transformation operations as tools for AI agents (Claude, GPT-4, and any MCP-compatible client).

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![MCP](https://img.shields.io/badge/protocol-MCP-green.svg)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub](https://img.shields.io/badge/github-sherman94062%2Fsqlmesh--mcp-lightgrey.svg)](https://github.com/sherman94062/sqlmesh-mcp)

---

## What is this?

**sqlmesh-mcp** lets an AI agent interact with a SQLMesh project through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). The agent can inspect models, generate and apply plans, run tests and audits, explore column-level lineage, manage environments, and execute SQL — all without leaving the AI chat interface.

This is particularly powerful because SQLMesh's column-level lineage (a key advantage over dbt) becomes directly queryable: ask an AI "where does the `revenue` column in `finance.daily_summary` come from?" and get a traced answer across the full DAG.

---

## Tools

| Tool | Description |
|---|---|
| `sqlmesh_model_list` | List all models with kind, cron, grain, tags |
| `sqlmesh_model_info` | Detailed metadata for a single model (columns, audits, query preview) |
| `sqlmesh_model_render` | Render fully-expanded SQL for a model |
| `sqlmesh_plan` | **Read-only** — preview what would change in an environment |
| `sqlmesh_apply` | Apply a plan to an environment (plan + auto-apply) |
| `sqlmesh_run` | Execute scheduled models whose cron interval is due |
| `sqlmesh_test` | Run SQLMesh unit tests (fixture-based, no warehouse) |
| `sqlmesh_audit` | Run data quality audits against the warehouse |
| `sqlmesh_dag` | Traverse the model DAG (upstream/downstream) |
| `sqlmesh_lineage` | **Column-level lineage** — trace column provenance across hops |
| `sqlmesh_environment_list` | List all environments and their state |
| `sqlmesh_diff` | Diff an environment against prod |
| `sqlmesh_table_diff` | Row-level data diff between two environments |
| `sqlmesh_fetchdf` | Execute SQL and return results as JSON |
| `sqlmesh_invalidate_environment` | Expire a dev/feature environment |

---

## Installation

### From source (recommended during development)

```bash
git clone https://github.com/arthursherman/sqlmesh-mcp
cd sqlmesh-mcp
pip install -e ".[dev]"
```

### From PyPI (once published)

```bash
pip install sqlmesh-mcp
```

---

## Configuration

The server is configured through environment variables:

| Variable | Description | Default |
|---|---|---|
| `SQLMESH_PROJECT_PATH` | Absolute path to your SQLMesh project | Current working directory |
| `SQLMESH_GATEWAY` | Named gateway defined in `config.yaml` (optional) | Default gateway |

---

## Usage

### With Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sqlmesh": {
      "command": "python",
      "args": ["-m", "sqlmesh_mcp.server"],
      "env": {
        "SQLMESH_PROJECT_PATH": "/path/to/your/sqlmesh/project"
      }
    }
  }
}
```

### With Claude Code

```bash
claude mcp add sqlmesh -- python -m sqlmesh_mcp.server
```

Set the project path via:

```bash
export SQLMESH_PROJECT_PATH=/path/to/your/project
```

### With MCP Inspector (recommended for development)

```bash
SQLMESH_PROJECT_PATH=/path/to/project \
  npx @modelcontextprotocol/inspector python -m sqlmesh_mcp.server
```

Open `http://localhost:6274` in your browser to interactively test every tool.

---

## Example interactions

Once connected to Claude or another MCP client, you can ask:

```
"List all models in my SQLMesh project"
"What columns does db.orders have and what are their types?"
"Show me a plan for the dev environment"
"Where does the revenue column in finance.daily_summary come from?"
"Run tests for the orders model"
"What's the difference between prod and dev right now?"
"Execute: SELECT COUNT(*) FROM db.orders WHERE status = 'complete'"
```

---

## Development

### Running tests

```bash
pytest tests/
```

### Linting

```bash
ruff check src/
```

### Testing against a real SQLMesh project

The fastest way to get started is with SQLMesh's built-in DuckDB quickstart:

```bash
mkdir sqlmesh-demo && cd sqlmesh-demo
pip install sqlmesh
sqlmesh init duckdb
# Then point this server at it:
SQLMESH_PROJECT_PATH=$(pwd) npx @modelcontextprotocol/inspector python -m sqlmesh_mcp.server
```

---

## Security considerations

- This server executes SQLMesh operations with the permissions of the process user.
- The `sqlmesh_apply` and `sqlmesh_run` tools **modify data** — consider wrapping with a governance layer (see [Castellan](https://github.com/arthursherman/castellan-governance-mcp)) if deploying in a shared or production context.
- The `sqlmesh_fetchdf` tool executes arbitrary SQL — restrict access accordingly.
- Bind the MCP Inspector proxy only to localhost; do **not** expose it publicly.

---

## Roadmap

- [ ] dbt-to-SQLMesh migration assistant tool
- [ ] Semantic model / metric exposure via MCP resources
- [ ] CI/CD integration (GitHub Actions plan summary)
- [ ] Multi-project support (monorepo)
- [ ] MCP sampling for agentic backfill decision-making

---

## Related projects

- [castellan-governance-mcp](https://github.com/sherman94062/castellan-governance-mcp) — governance and security wrapper for MCP servers
- [SQLMesh](https://github.com/TobikoData/sqlmesh) — the underlying data transformation framework

---

## License

MIT — see [LICENSE](LICENSE).
