#!/usr/bin/env python3
"""
SQLMesh MCP Server
==================
Exposes SQLMesh project operations as MCP tools for AI agents.

Supports:
  - Model inspection (list, info, render)
  - Plan generation (preview changes, no side effects)
  - Apply / Run / Backfill
  - Unit tests and data audits
  - DAG traversal (upstream / downstream dependencies)
  - Column-level lineage (via SQLMesh CLI)
  - Environment management (list, diff, invalidate)
  - Table diff between environments
  - Direct SQL execution via fetchdf

Configuration (environment variables):
  SQLMESH_PROJECT_PATH  Path to the SQLMesh project directory (default: cwd)
  SQLMESH_GATEWAY       Named gateway to use (optional)
"""

import asyncio
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP

# ── Server declaration ────────────────────────────────────────────────────────

mcp = FastMCP(
    "sqlmesh-mcp",
    instructions=(
        "MCP server for SQLMesh — the next-generation data transformation framework. "
        "Provides tools for model inspection, planning, execution, testing, lineage, "
        "and environment management."
    ),
)

# ── Internal helpers ──────────────────────────────────────────────────────────


def _project_path(project_path: Optional[str] = None) -> str:
    return str(
        Path(
            project_path or os.environ.get("SQLMESH_PROJECT_PATH", ".")
        ).resolve()
    )


async def _run_python_script(script: str, project_path: Optional[str] = None) -> str:
    """
    Run a Python script in a subprocess that imports and uses SQLMesh.

    This avoids SQLMesh's Context interfering with the MCP server's event loop.
    The script should print its result to stdout as the last line.
    """
    cwd = _project_path(project_path)
    gateway_env = os.environ.get("SQLMESH_GATEWAY", "")

    wrapper = f"""
import sys, os, json, traceback
os.chdir({cwd!r})
os.environ['SQLMESH_PROJECT_PATH'] = {cwd!r}
if {gateway_env!r}:
    os.environ['SQLMESH_GATEWAY'] = {gateway_env!r}

try:
    from sqlmesh import Context
    _kwargs = {{"paths": [{cwd!r}]}}
    if os.environ.get("SQLMESH_GATEWAY"):
        _kwargs["gateway"] = os.environ["SQLMESH_GATEWAY"]
    ctx = Context(**_kwargs)

{script}
except Exception as exc:
    print(json.dumps({{"error": str(exc), "traceback": traceback.format_exc()}}))
"""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", wrapper,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    output = stdout.decode().strip()
    if not output:
        err = stderr.decode().strip()
        return json.dumps({"error": f"No output from subprocess", "stderr": err[-500:]})
    return output


async def _run_cli(args: list[str], project_path: Optional[str] = None) -> str:
    """
    Run a SQLMesh CLI command in the project directory and return combined output.
    """
    cwd = _project_path(project_path)
    cmd = [sys.executable, "-m", "sqlmesh"] + args
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    output = stdout.decode().strip()
    if proc.returncode != 0 and stderr:
        output = (output + "\n" + stderr.decode().strip()).strip()
    return output or "(no output)"


# ── Tools ─────────────────────────────────────────────────────────────────────


@mcp.tool()
async def sqlmesh_model_list(project_path: Optional[str] = None) -> str:
    """
    List all models in the SQLMesh project.

    Returns each model's name, kind (FULL, INCREMENTAL_BY_TIME_RANGE, etc.),
    cron schedule, SQL dialect, grain columns, and tags.

    Args:
        project_path: Path to the SQLMesh project. Defaults to
                      SQLMESH_PROJECT_PATH env var or the current directory.
    """
    script = """
    rows = []
    for name, model in sorted(ctx.models.items()):
        row = {"name": str(name)}
        if hasattr(model, "kind"):
            row["kind"] = str(model.kind.name)
        if hasattr(model, "cron") and model.cron:
            row["cron"] = str(model.cron)
        if hasattr(model, "dialect") and model.dialect:
            row["dialect"] = str(model.dialect)
        if hasattr(model, "grain") and model.grain:
            row["grain"] = [str(g) for g in model.grain]
        if hasattr(model, "tags") and model.tags:
            row["tags"] = sorted(str(t) for t in model.tags)
        rows.append(row)
    print(json.dumps(rows, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_model_info(
    model_name: str,
    project_path: Optional[str] = None,
) -> str:
    """
    Return detailed metadata for a single SQLMesh model.

    Includes kind, cron, grain, column types, audits, partition columns,
    and a preview of the model's SQL query.

    Args:
        model_name:   Fully-qualified model name, e.g. ``"db.my_model"``.
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    model = ctx.get_model({model_name!r})
    if model is None:
        available = sorted(str(k) for k in ctx.models.keys())
        print(json.dumps({{"error": "Model not found", "available": available}}, indent=2))
    else:
        info = {{"name": str(model.name)}}
        for attr in ("kind", "cron", "dialect", "stamp"):
            val = getattr(model, attr, None)
            if val is not None:
                info[attr] = str(val.name) if attr == "kind" else str(val)
        for list_attr in ("grain", "tags", "audits"):
            val = getattr(model, list_attr, None)
            if val:
                info[list_attr] = [str(v) for v in val]
        if hasattr(model, "partitioned_by_") and model.partitioned_by_:
            info["partitioned_by"] = [str(p) for p in model.partitioned_by_]
        if hasattr(model, "columns_to_types") and model.columns_to_types:
            info["columns"] = {{k: str(v) for k, v in model.columns_to_types.items()}}
        if hasattr(model, "query") and model.query is not None:
            q = str(model.query)
            info["query_preview"] = q[:600] + ("  [truncated]" if len(q) > 600 else "")
        print(json.dumps(info, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_model_render(
    model_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Render the fully-expanded SQL for a model (Jinja evaluated, CTEs inlined).

    Useful for understanding exactly what SQL will be executed against the warehouse.

    Args:
        model_name:   Fully-qualified model name, e.g. ``"db.my_model"``.
        start:        Optional start date for time-based rendering (e.g. ``"2024-01-01"``).
        end:          Optional end date.
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    kwargs = {{}}
    if {start!r}:
        kwargs["start"] = {start!r}
    if {end!r}:
        kwargs["end"] = {end!r}
    expr = ctx.render({model_name!r}, **kwargs)
    print(str(expr))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_plan(
    environment: str = "dev",
    start: Optional[str] = None,
    end: Optional[str] = None,
    select_models: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Generate a SQLMesh plan showing what changes would be applied to an environment.

    **Read-only** — does NOT modify any data or environment state.
    Returns new, modified, and removed models plus required backfill intervals.

    Args:
        environment:   Target environment name (default: ``"dev"``).
        start:         Optional backfill start date (e.g. ``"2024-01-01"``).
        end:           Optional backfill end date.
        select_models: Comma-separated model names to limit the plan scope.
        project_path:  Path to the SQLMesh project.
    """
    models_list = f"[m.strip() for m in {select_models!r}.split(',')]" if select_models else "None"
    script = f"""
    kwargs = {{
        "environment": {environment!r},
        "no_prompts": True,
        "auto_apply": False,
    }}
    if {start!r}:
        kwargs["start"] = {start!r}
    if {end!r}:
        kwargs["end"] = {end!r}
    select = {models_list}
    if select:
        kwargs["select_models"] = select

    plan = ctx.plan(**kwargs)

    result = {{
        "environment": {environment!r},
        "has_changes": bool(plan.has_changes),
        "requires_backfill": bool(plan.requires_backfill),
        "new_models": [],
        "modified_models": {{}},
        "missing_intervals": [],
    }}

    if plan.new_snapshots:
        result["new_models"] = [str(s.name) for s in plan.new_snapshots]

    if plan.modified_snapshots:
        for name, (current, previous) in plan.modified_snapshots.items():
            result["modified_models"][str(name)] = {{
                "current_change_category": str(current.change_category)
                if hasattr(current, "change_category")
                else None,
            }}

    if plan.missing_intervals:
        for snapshot, intervals in plan.missing_intervals:
            result["missing_intervals"].append({{
                "model": str(snapshot.name),
                "interval_count": len(intervals),
            }})

    if not result["has_changes"] and not result["requires_backfill"]:
        result["status"] = "Environment is up to date — no changes detected."

    print(json.dumps(result, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_apply(
    environment: str = "dev",
    start: Optional[str] = None,
    end: Optional[str] = None,
    select_models: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Apply a SQLMesh plan to an environment (plan + auto-apply in one step).

    This modifies the target environment.  Use ``sqlmesh_plan`` first to
    preview changes before calling this tool.

    Args:
        environment:   Target environment (default: ``"dev"``).
        start:         Optional backfill start date.
        end:           Optional backfill end date.
        select_models: Comma-separated model names to limit scope.
        project_path:  Path to the SQLMesh project.
    """
    models_list = f"[m.strip() for m in {select_models!r}.split(',')]" if select_models else "None"
    script = f"""
    import io
    from contextlib import redirect_stdout
    kwargs = {{
        "environment": {environment!r},
        "no_prompts": True,
        "auto_apply": True,
    }}
    if {start!r}:
        kwargs["start"] = {start!r}
    if {end!r}:
        kwargs["end"] = {end!r}
    select = {models_list}
    if select:
        kwargs["select_models"] = select

    buf = io.StringIO()
    with redirect_stdout(buf):
        ctx.plan(**kwargs)
    output = buf.getvalue().strip()
    print(output or "Changes applied to environment '{environment}' successfully.")
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_run(
    environment: str = "prod",
    start: Optional[str] = None,
    end: Optional[str] = None,
    select_models: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Execute scheduled SQLMesh models whose cron interval is due.

    Typically used against the ``prod`` environment to advance the pipeline.

    Args:
        environment:   Target environment (default: ``"prod"``).
        start:         Optional execution start boundary.
        end:           Optional execution end boundary.
        select_models: Comma-separated model names to limit execution.
        project_path:  Path to the SQLMesh project.
    """
    models_list = f"[m.strip() for m in {select_models!r}.split(',')]" if select_models else "None"
    script = f"""
    import io
    from contextlib import redirect_stdout
    kwargs = {{"environment": {environment!r}}}
    if {start!r}:
        kwargs["start"] = {start!r}
    if {end!r}:
        kwargs["end"] = {end!r}
    select = {models_list}
    if select:
        kwargs["select_models"] = select

    buf = io.StringIO()
    with redirect_stdout(buf):
        ctx.run(**kwargs)
    output = buf.getvalue().strip()
    print(output or "Run completed for environment '{environment}'.")
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_test(
    match_patterns: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Run SQLMesh unit tests.

    SQLMesh unit tests validate model logic using fixture data without
    touching the warehouse.

    Args:
        match_patterns: Comma-separated glob patterns to filter tests
                        (e.g. ``"test_orders,test_customers"``).
                        If omitted, all tests are run.
        project_path:   Path to the SQLMesh project.
    """
    patterns_list = f"[p.strip() for p in {match_patterns!r}.split(',')]" if match_patterns else "None"
    script = f"""
    import io
    kwargs = {{}}
    patterns = {patterns_list}
    if patterns:
        kwargs["match_patterns"] = patterns

    buf = io.StringIO()
    result = ctx.test(stream=buf, **kwargs)
    output = buf.getvalue().strip()

    passed = result.testsRun - len(result.failures) - len(result.errors)
    summary = {{
        "tests_run": result.testsRun,
        "passed": passed,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "output": output,
    }}
    if result.failures:
        summary["failure_details"] = [
            {{"test": str(t), "message": msg}} for t, msg in result.failures
        ]
    if result.errors:
        summary["error_details"] = [
            {{"test": str(t), "message": msg}} for t, msg in result.errors
        ]
    print(json.dumps(summary, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_audit(
    start: str,
    end: str,
    models: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Run SQLMesh data audits to validate data quality constraints.

    Audits check conditions like NOT_NULL, UNIQUE_VALUES, and custom assertions
    defined in model files.

    Args:
        start:        Start date for the audit window (e.g. ``"2024-01-01"``). Required.
        end:          End date for the audit window (e.g. ``"2024-12-31"``). Required.
        models:       Comma-separated model names to audit. If omitted, audits all.
        project_path: Path to the SQLMesh project.
    """
    models_list = f"iter([m.strip() for m in {models!r}.split(',')])" if models else "None"
    script = f"""
    import io
    from contextlib import redirect_stdout
    kwargs = {{"start": {start!r}, "end": {end!r}}}
    models_iter = {models_list}
    if models_iter is not None:
        kwargs["models"] = models_iter

    buf = io.StringIO()
    with redirect_stdout(buf):
        ctx.audit(**kwargs)
    output = buf.getvalue().strip()
    print(output or "All audits passed.")
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_dag(
    model_name: Optional[str] = None,
    direction: str = "both",
    project_path: Optional[str] = None,
) -> str:
    """
    Explore the SQLMesh model DAG (directed acyclic graph).

    Returns upstream and/or downstream dependencies for a model,
    or the full DAG if no model name is given.

    Args:
        model_name:   Model to inspect (e.g. ``"db.orders"``).
                      If omitted, returns the full project DAG.
        direction:    ``"upstream"``, ``"downstream"``, or ``"both"`` (default).
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    dag = ctx.dag
    model_name = {model_name!r}
    direction = {direction!r}

    if model_name:
        result = {{"model": model_name}}
        if direction in ("upstream", "both"):
            result["upstream"] = [str(n) for n in sorted(dag.upstream(model_name))]
        if direction in ("downstream", "both"):
            result["downstream"] = [str(n) for n in dag.downstream(model_name)]
    else:
        result = {{}}
        for node, deps in dag.graph.items():
            result[str(node)] = {{
                "upstream": [str(d) for d in sorted(deps)],
                "downstream": [str(n) for n in dag.downstream(node)],
            }}

    print(json.dumps(result, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_lineage(
    model_name: str,
    column_name: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Retrieve column-level lineage — one of SQLMesh's key differentiators over dbt.

    Traces exactly which upstream columns feed into each column of the target model,
    across any number of hops in the DAG.

    Args:
        model_name:   Fully-qualified model name (e.g. ``"db.orders"``).
        column_name:  Specific column to trace. If omitted, traces all columns.
        project_path: Path to the SQLMesh project.
    """
    args = ["lineage", model_name]
    if column_name:
        args += ["--column", column_name]
    return await _run_cli(args, project_path)


@mcp.tool()
async def sqlmesh_environment_list(project_path: Optional[str] = None) -> str:
    """
    List all SQLMesh environments and their snapshot counts.

    Environments represent isolated views of the pipeline state (e.g. dev, staging, prod).

    Args:
        project_path: Path to the SQLMesh project.
    """
    script = """
    envs = ctx.state_reader.get_environments()
    result = []
    for env in envs:
        entry = {"name": str(env.name)}
        if hasattr(env, "snapshots"):
            entry["snapshot_count"] = len(env.snapshots)
        if hasattr(env, "expiration_ts") and env.expiration_ts:
            entry["expiration_ts"] = str(env.expiration_ts)
        if hasattr(env, "suffix_target") and env.suffix_target:
            entry["suffix_target"] = str(env.suffix_target)
        result.append(entry)
    print(json.dumps(result, indent=2))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_diff(
    environment: str = "dev",
    detailed: bool = False,
    project_path: Optional[str] = None,
) -> str:
    """
    Show the diff between the given environment and production.

    Highlights which models differ between the two states.

    Args:
        environment:  Environment to compare against prod (default: ``"dev"``).
        detailed:     Include detailed column-level diff (default: False).
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        ctx.diff(environment={environment!r}, detailed={detailed!r})
    output = buf.getvalue().strip()
    print(output or "No differences between '{environment}' and prod.")
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_table_diff(
    source: str,
    target: str,
    model_name: Optional[str] = None,
    on_columns: Optional[str] = None,
    limit: int = 20,
    project_path: Optional[str] = None,
) -> str:
    """
    Compare data in a model between two environments row-by-row.

    Useful for validating that a code change produces the expected data output.

    Args:
        source:       Source environment (e.g. ``"prod"``).
        target:       Target environment (e.g. ``"dev"``).
        model_name:   Model to diff. Required.
        on_columns:   Comma-separated join key columns (e.g. ``"id,date"``).
        limit:        Maximum differing rows to return (default: 20).
        project_path: Path to the SQLMesh project.
    """
    on_list = f"[c.strip() for c in {on_columns!r}.split(',')]" if on_columns else "None"
    script = f"""
    import io
    from contextlib import redirect_stdout
    kwargs = {{
        "source": {source!r},
        "target": {target!r},
        "limit": {limit!r},
        "show": False,
    }}
    if {model_name!r}:
        kwargs["select_models"] = [{model_name!r}]
    on = {on_list}
    if on:
        kwargs["on"] = on

    buf = io.StringIO()
    with redirect_stdout(buf):
        ctx.table_diff(**kwargs)
    output = buf.getvalue().strip()
    print(output or "No differences found between the two environments.")
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_fetchdf(
    query: str,
    limit: int = 100,
    project_path: Optional[str] = None,
) -> str:
    """
    Execute a SQL query against the warehouse and return results as JSON.

    Uses the SQLMesh project's configured gateway / connection.

    Args:
        query:        SQL query to execute (any dialect supported by the gateway).
        limit:        Maximum rows to return (default: 100).
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    q = {query!r}.strip().rstrip(";")
    if "limit" not in q.lower():
        q = f"{{q}} LIMIT {limit}"
    df = ctx.fetchdf(q)
    result = {{
        "row_count": len(df),
        "columns": list(df.columns),
        "rows": df.head({limit}).to_dict(orient="records"),
    }}
    print(json.dumps(result, indent=2, default=str))
"""
    return await _run_python_script(script, project_path)


@mcp.tool()
async def sqlmesh_invalidate_environment(
    environment: str,
    project_path: Optional[str] = None,
) -> str:
    """
    Invalidate (expire) a SQLMesh virtual environment, marking it for cleanup.

    Use this to retire dev or feature-branch environments when they are no longer needed.

    Args:
        environment:  Name of the environment to invalidate (e.g. ``"dev"``).
        project_path: Path to the SQLMesh project.
    """
    script = f"""
    ctx.invalidate_environment({environment!r})
    print("Environment '{environment}' has been invalidated and scheduled for cleanup.")
"""
    return await _run_python_script(script, project_path)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
