#!/bin/sh
# shellcheck disable=SC2164
cd "$(dirname "$0")";
CWD="$(pwd)"
echo $CWD
python etl.py