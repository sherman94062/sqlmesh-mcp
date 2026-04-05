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

import io
import json
import os
import subprocess
import sys
import traceback
from contextlib import redirect_stdout, redirect_stderr
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


def _get_context(project_path: Optional[str] = None):
    """
    Create and return a SQLMesh Context.

    Resolution order for the project path:
      1. Explicit ``project_path`` argument
      2. ``SQLMESH_PROJECT_PATH`` environment variable
      3. Current working directory
    """
    from sqlmesh import Context

    path = Path(
        project_path
        or os.environ.get("SQLMESH_PROJECT_PATH", ".")
    ).resolve()

    kwargs: dict[str, Any] = {"paths": [path]}
    gateway = os.environ.get("SQLMESH_GATEWAY")
    if gateway:
        kwargs["gateway"] = gateway

    return Context(**kwargs)


def _capture_stdout(fn, *args, **kwargs) -> tuple[Any, str]:
    """
    Execute *fn* while capturing everything it prints to stdout.

    Returns (return_value, captured_text).
    """
    buf = io.StringIO()
    with redirect_stdout(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue().strip()


def _run_cli(args: list[str], project_path: Optional[str] = None) -> str:
    """
    Run a SQLMesh CLI command in the project directory and return combined output.
    Used for operations that are easier to consume via CLI output (e.g. lineage).
    """
    cwd = str(
        Path(
            project_path or os.environ.get("SQLMESH_PROJECT_PATH", ".")
        ).resolve()
    )
    cmd = [sys.executable, "-m", "sqlmesh"] + args
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
    )
    output = result.stdout.strip()
    if result.returncode != 0 and result.stderr:
        output = (output + "\n" + result.stderr.strip()).strip()
    return output or "(no output)"


# ── Tools ─────────────────────────────────────────────────────────────────────


@mcp.tool()
def sqlmesh_model_list(project_path: Optional[str] = None) -> str:
    """
    List all models in the SQLMesh project.

    Returns each model's name, kind (FULL, INCREMENTAL_BY_TIME_RANGE, etc.),
    cron schedule, SQL dialect, grain columns, and tags.

    Args:
        project_path: Path to the SQLMesh project. Defaults to
                      SQLMESH_PROJECT_PATH env var or the current directory.
    """
    try:
        ctx = _get_context(project_path)
        rows = []
        for name, model in sorted(ctx.models.items()):
            row: dict[str, Any] = {"name": str(name)}
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
        return json.dumps(rows, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_model_info(
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
    try:
        ctx = _get_context(project_path)
        model = ctx.get_model(model_name)
        if model is None:
            available = sorted(ctx.models.keys())
            return (
                f"Model '{model_name}' not found.\n"
                f"Available models:\n{json.dumps(available, indent=2)}"
            )

        info: dict[str, Any] = {"name": str(model.name)}

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
            info["columns"] = {k: str(v) for k, v in model.columns_to_types.items()}

        if hasattr(model, "query") and model.query is not None:
            q = str(model.query)
            info["query_preview"] = q[:600] + ("  [truncated]" if len(q) > 600 else "")

        return json.dumps(info, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_model_render(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {}
        if start:
            kwargs["start"] = start
        if end:
            kwargs["end"] = end
        expr = ctx.render(model_name, **kwargs)
        return str(expr)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_plan(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {
            "environment": environment,
            "no_prompts": True,
            "auto_apply": False,
        }
        if start:
            kwargs["start"] = start
        if end:
            kwargs["end"] = end
        if select_models:
            kwargs["select_models"] = [m.strip() for m in select_models.split(",")]

        _, _ = _capture_stdout(lambda: None)  # warm up
        plan = ctx.plan(**kwargs)

        result: dict[str, Any] = {
            "environment": environment,
            "has_changes": bool(plan.has_changes),
            "requires_backfill": bool(plan.requires_backfill),
            "new_models": [],
            "modified_models": {},
            "missing_intervals": [],
        }

        if plan.new_snapshots:
            result["new_models"] = [str(s.name) for s in plan.new_snapshots]

        if plan.modified_snapshots:
            for name, (current, previous) in plan.modified_snapshots.items():
                result["modified_models"][str(name)] = {
                    "current_change_category": str(current.change_category)
                    if hasattr(current, "change_category")
                    else None,
                }

        if plan.missing_intervals:
            for snapshot, intervals in plan.missing_intervals:
                result["missing_intervals"].append(
                    {
                        "model": str(snapshot.name),
                        "interval_count": len(intervals),
                    }
                )

        if not result["has_changes"] and not result["requires_backfill"]:
            result["status"] = "Environment is up to date — no changes detected."

        return json.dumps(result, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_apply(
    environment: str = "dev",
    start: Optional[str] = None,
    end: Optional[str] = None,
    select_models: Optional[str] = None,
    project_path: Optional[str] = None,
) -> str:
    """
    Apply a SQLMesh plan to an environment (plan + auto-apply in one step).

    ⚠️  This modifies the target environment.  Use ``sqlmesh_plan`` first to
    preview changes before calling this tool.

    Args:
        environment:   Target environment (default: ``"dev"``).
        start:         Optional backfill start date.
        end:           Optional backfill end date.
        select_models: Comma-separated model names to limit scope.
        project_path:  Path to the SQLMesh project.
    """
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {
            "environment": environment,
            "no_prompts": True,
            "auto_apply": True,
        }
        if start:
            kwargs["start"] = start
        if end:
            kwargs["end"] = end
        if select_models:
            kwargs["select_models"] = [m.strip() for m in select_models.split(",")]

        _, output = _capture_stdout(ctx.plan, **kwargs)
        return output or f"Changes applied to environment '{environment}' successfully."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_run(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {"environment": environment}
        if start:
            kwargs["start"] = start
        if end:
            kwargs["end"] = end
        if select_models:
            kwargs["select_models"] = [m.strip() for m in select_models.split(",")]

        _, output = _capture_stdout(ctx.run, **kwargs)
        return output or f"Run completed for environment '{environment}'."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_test(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {}
        if match_patterns:
            kwargs["match_patterns"] = [p.strip() for p in match_patterns.split(",")]

        buf = io.StringIO()
        result = ctx.test(stream=buf, **kwargs)
        output = buf.getvalue().strip()

        passed = result.testsRun - len(result.failures) - len(result.errors)
        summary = {
            "tests_run": result.testsRun,
            "passed": passed,
            "failures": len(result.failures),
            "errors": len(result.errors),
            "output": output,
        }
        if result.failures:
            summary["failure_details"] = [
                {"test": str(t), "message": msg} for t, msg in result.failures
            ]
        if result.errors:
            summary["error_details"] = [
                {"test": str(t), "message": msg} for t, msg in result.errors
            ]
        return json.dumps(summary, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_audit(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {"start": start, "end": end}
        if models:
            import itertools
            kwargs["models"] = iter([m.strip() for m in models.split(",")])

        _, output = _capture_stdout(ctx.audit, **kwargs)
        return output or "All audits passed."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_dag(
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
    try:
        ctx = _get_context(project_path)
        dag = ctx.dag

        if model_name:
            result: dict[str, Any] = {"model": model_name}
            if direction in ("upstream", "both"):
                result["upstream"] = [str(n) for n in sorted(dag.upstream(model_name))]
            if direction in ("downstream", "both"):
                result["downstream"] = [str(n) for n in dag.downstream(model_name)]
        else:
            # Full DAG — one entry per node
            result = {}
            for node, deps in dag.graph.items():
                result[str(node)] = {
                    "upstream": [str(d) for d in sorted(deps)],
                    "downstream": [str(n) for n in dag.downstream(node)],
                }

        return json.dumps(result, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_lineage(
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
    try:
        args = ["lineage", model_name]
        if column_name:
            args += ["--column", column_name]
        return _run_cli(args, project_path)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_environment_list(project_path: Optional[str] = None) -> str:
    """
    List all SQLMesh environments and their snapshot counts.

    Environments represent isolated views of the pipeline state (e.g. dev, staging, prod).

    Args:
        project_path: Path to the SQLMesh project.
    """
    try:
        ctx = _get_context(project_path)
        envs = ctx.state_reader.get_environments()
        result = []
        for env in envs:
            entry: dict[str, Any] = {"name": str(env.name)}
            if hasattr(env, "snapshots"):
                entry["snapshot_count"] = len(env.snapshots)
            if hasattr(env, "expiration_ts") and env.expiration_ts:
                entry["expiration_ts"] = str(env.expiration_ts)
            if hasattr(env, "suffix_target") and env.suffix_target:
                entry["suffix_target"] = str(env.suffix_target)
            result.append(entry)
        return json.dumps(result, indent=2)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_diff(
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
    try:
        ctx = _get_context(project_path)
        _, output = _capture_stdout(ctx.diff, environment=environment, detailed=detailed)
        return output or f"No differences between '{environment}' and prod."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_table_diff(
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
    try:
        ctx = _get_context(project_path)
        kwargs: dict[str, Any] = {
            "source": source,
            "target": target,
            "limit": limit,
            "show": False,
        }
        if model_name:
            kwargs["select_models"] = [model_name]
        if on_columns:
            kwargs["on"] = [c.strip() for c in on_columns.split(",")]

        _, output = _capture_stdout(ctx.table_diff, **kwargs)
        return output or "No differences found between the two environments."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_fetchdf(
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
    try:
        ctx = _get_context(project_path)
        q = query.strip().rstrip(";")
        if "limit" not in q.lower():
            q = f"{q} LIMIT {limit}"
        df = ctx.fetchdf(q)
        result = {
            "row_count": len(df),
            "columns": list(df.columns),
            "rows": df.head(limit).to_dict(orient="records"),
        }
        return json.dumps(result, indent=2, default=str)
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


@mcp.tool()
def sqlmesh_invalidate_environment(
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
    try:
        ctx = _get_context(project_path)
        ctx.invalidate_environment(environment)
        return f"Environment '{environment}' has been invalidated and scheduled for cleanup."
    except Exception as exc:
        return f"Error: {exc}\n{traceback.format_exc()}"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
