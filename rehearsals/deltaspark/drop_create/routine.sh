#! /usr/bin/env bash

set -euxo pipefail

echo "Installing requirements"
python setup.py install --user --style deltaspark

echo "Preparing database"
python rehearsals/deltaspark/drop_create/prepare.py

echo "Running first ddl"
tabledancer dance rehearsals/deltaspark/drop_create/table_before.yaml

echo "Running updated ddl"
tabledancer dance rehearsals/deltaspark/drop_create/table_after.yaml