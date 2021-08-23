#! /usr/bin/env bash

set -euxo pipefail

python setup.py install --user --style deltaspark

tabledancer dance rehearsals/deltaspark/drop_create/table_before.yaml

tabledancer dance rehearsals/deltaspark/drop_create/table_after.yaml