#!/bin/sh
PYTHONPATH="$(pwd):$(pwd)/test"

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
