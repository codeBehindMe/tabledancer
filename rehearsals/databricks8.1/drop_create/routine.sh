#! /usr/bin/env bash

set -exo pipefail

echo "Installing requirements"
python setup.py install --user --style databricks8.1

echo {\"host\":\"$DATABRICKS_ADDRESS\", \"token\":\"$DATABRICKS_API_TOKEN\", \"cluster_id\":\"$DATABRICKS_CLUSTER_ID\",\"org_id\":\"0\",\"port\":\"15001\"} > $HOME/.databricks-connect

echo "Preparing database"
python rehearsals/databricks8.1/drop_create/prepare.py

echo "Running first ddl"
tabledancer dance rehearsals/databricks8.1/drop_create/table_before.yaml

echo "Running updated ddl"
tabledancer dance rehearsals/databricks8.1/drop_create/table_after.yaml