#! /usr/bin/env bash

set -euxo pipefail

echo "Installing requirements"
python setup.py install --user --style databricks8.1

databricks-connect configure -y

echo "Preparing database"
python rehearsals/databricks8.1/drop_create/prepare.py

echo "Running first ddl"
tabledancer dance rehearsals/databricks8.1/drop_create/table_before.yaml

echo "Running updated ddl"
tabledancer dance rehearsals/databricks8.1/drop_create/table_after.yaml