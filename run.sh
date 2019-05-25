#!/bin/bash

export jid=`databricks jobs list | grep "Daily flow" | awk '{print $1}'`

databricks jobs run-now --job-id $jid
