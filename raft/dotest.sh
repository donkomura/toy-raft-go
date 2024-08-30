#!/bin/bash
set -ex

logfile=/tmp/rlog-$(date "+%Y%m%d.%H%M%S")

go test -v -race -run $@ |& tee ${logfile}

go run ../tools/raft-testlog-viz/main.go < ${logfile}