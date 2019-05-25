#!/bin/bash

databricks workspace import -o -f SOURCE -l PYTHON dp/flow.py /Users/debajyoti.roy@databricks.com/hw/flow

python setup.py bdist_wheel

databricks fs cp --overwrite dist/lib-0.1-py2-none-any.whl dbfs:/tmp/roy/hw_lib/lib-0.1-py2-none-any.whl

export jid=`databricks jobs create --json-file job_spec.json | sed -n 2p | awk '{print $2}'`

databricks jobs run-now --job-id $jid
