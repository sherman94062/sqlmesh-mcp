#!/bin/bash
# Run this from inside the sqlmesh-mcp directory on your Mac
# Requires: git installed, GitHub account authenticated (gh CLI or HTTPS token)

set -e

REPO_NAME="sqlmesh-mcp"
GITHUB_USER="sherman94062"

echo "🚀 Pushing $REPO_NAME to github.com/$GITHUB_USER/$REPO_NAME"

# Initialize git
git init
git add .
git commit -m "feat: initial SQLMesh MCP server with 15 tools

- sqlmesh_model_list, sqlmesh_model_info, sqlmesh_model_render
- sqlmesh_plan (read-only preview), sqlmesh_apply, sqlmesh_run
- sqlmesh_test, sqlmesh_audit
- sqlmesh_dag (DAG traversal), sqlmesh_lineage (column-level)
- sqlmesh_environment_list, sqlmesh_diff, sqlmesh_table_diff
- sqlmesh_fetchdf, sqlmesh_invalidate_environment"

# Create repo on GitHub and push (requires gh CLI)
if command -v gh &> /dev/null; then
    gh repo create "$REPO_NAME" --public --push --source=.
else
    echo ""
    echo "gh CLI not found. Run these commands manually:"
    echo ""
    echo "  1. Create repo at: https://github.com/new (name: $REPO_NAME, public, no README)"
    echo ""
    echo "  2. Then run:"
    echo "     git remote add origin https://github.com/$GITHUB_USER/$REPO_NAME.git"
    echo "     git branch -M main"
    echo "     git push -u origin main"
fi

echo ""
echo "✅ Done! Repo will be at: https://github.com/$GITHUB_USER/$REPO_NAME"
