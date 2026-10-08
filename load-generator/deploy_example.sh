#!/bin/bash

# Delete the function "task1" (attempted multiple times for safety)
for i in {1..4}; do
  fission fn delete --name=task1 2>/dev/null || echo "Attempt $i: task1 may not exist"
done

# Change directory and deploy
cd ../generated-workflows/wf-1 || { echo "Directory not found"; exit 1; }
./deploy.sh
