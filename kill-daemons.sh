#!/bin/sh
ps ax | grep test_client_integration | awk '{print $1}' | xargs kill
rm -rf /tmp/supervisor
