[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![run-tests](https://github.com/codeBehindMe/tabledancer/actions/workflows/run-tests.yaml/badge.svg)](https://github.com/codeBehindMe/tabledancer/actions/workflows/run-tests.yaml)
[![run-walkthroughs](https://github.com/codeBehindMe/tabledancer/actions/workflows/run-walkthroughs.yaml/badge.svg)](https://github.com/codeBehindMe/tabledancer/actions/workflows/run-walkthroughs.yaml)

![logo](https://storage.googleapis.com/tabledancermedia/tabledancer_header_open_blush_large.png)

---
Tabledancer is a simple application which allows you to manage table lifecycles 
based on YAML inside CICD.

## Installation

Currently we don't have this distributed in pip as it's still in early 
development. Instead we'll have to install from source. Simply clone down the
repo using your favourite clone command.

Since this was borne out of work in databricks, and remote working with 
databricks / spark is a bit finicky, i.e. different versions of 
`databricks-connect` required for working with different cluster runtimes; we
have provided a special `style` installation option to get the correct 
requirements.

> It's highly recommended that you do this inside a `virtualenv` or a container
> since databricks-connect plays funny with pyspark. 

Install just like below, use the `--style` flag to provide which backend.

```
python setup.py install --user --style=databricks8.1
```