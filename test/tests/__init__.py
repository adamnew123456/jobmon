import logging

# Since the supervisor uses the logging module, dump all the messages since
# we're not testing those
logging.basicConfig(filename='/dev/null')
