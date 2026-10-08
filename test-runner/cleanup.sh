#!/bin/bash

# A robust script to clean up Fission resources.
#
# Usage:
#   ./cleanup.sh        - Deletes ALL workflow-related functions and routes.
#   ./cleanup.sh [WF_ID] - Deletes functions and routes for a specific workflow.

set +e # Don't exit immediately on error, as 'fission ... delete' might fail if resource doesn't exist.

WF_ID="$1"

if [ -n "$WF_ID" ]; then
  # --- TARGETED CLEANUP MODE ---
  echo "🧹 Performing targeted cleanup for workflow: $WF_ID"
  WF_DIR="workflows/$WF_ID"
  TASKS_JSON="$WF_DIR/tasks.json"

  if [ ! -f "$TASKS_JSON" ]; then
    echo "⚠️ tasks.json not found for $WF_ID, cannot clean up functions. Will only attempt to clean up route."
  else
    # Get task function names from tasks.json
    TASKS=$(python3 -c "import json; t=json.load(open('$TASKS_JSON'))['tasks']; print(' '.join(t.keys()))")
    
    # Delete Fission functions
    for fn in $TASKS; do
      echo " - Deleting function: $fn"
      fission fn delete --name $fn
    done
  fi

  # Delete Fission route
  echo " - Deleting route: ${WF_ID}-route"
  fission route delete --name=${WF_ID}-route

else
  # --- BLANKET CLEANUP MODE ---
  echo "🧹 Performing BLANKET cleanup of ALL workflow-related resources..."

  # Delete all workflow functions (names starting with 'task')
  echo " - Deleting all workflow functions..."
  fission fn list | grep -E '^task' | awk '{print $1}' | xargs -I {} fission fn delete --name {} || true

  # Delete all workflow routes (names ending with '-route')
  echo " - Deleting all workflow routes..."
  fission route list | awk '$1 ~ /-route$/ {print $1}' | while read route; do fission route delete --name "$route"; done || true
fi

set -e
echo "✅ Cleanup complete."