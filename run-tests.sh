#!/bin/sh
PYTHONPATH="$(pwd):$(pwd)/test"

# This is where the supervisor will put its sockets, so we have to make sure
# this directory is available
mkdir -p /tmp/supervisor

cd test
_TEST="$1"
if [ -n "$_TEST" ]; then
    echo "[Running tests/$_TEST.py]"
    python3 -m unittest tests/$_TEST.py
else
    for testfile in tests/test_*.py; do
        echo "[Running $testfile]"
        python3 -m unittest $testfile || exit 1
    done
fi

exec ./kill-daemons.sh
