import logging
import os

# Since the supervisor uses the logging module, dump all the messages since
# we're not testing those
logging.basicConfig(filename='/dev/null')

# The configuration file references an environment variable called $TMP
os.environ['TMP'] = '/tmp'
