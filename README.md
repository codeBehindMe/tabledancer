# Tabledancer

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)


Simple Table Lifecycle Mangement for Small projects.

![Tabledancer](https://images.unsplash.com/photo-1540324155974-7523202daa3f?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=658&q=80)

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