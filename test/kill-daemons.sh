#!/bin/sh
#
# Should the tests fail, it is possible that some of the daemons spawned
# will stick around. This is run after every test to ensure that none of the
# daemons outlive the tests.

ps ax | grep test_client_integration | awk '{print $1}' | xargs kill
rm -rf /tmp/supervisor
