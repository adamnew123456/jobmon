'''
***
Modified generic daemon class
***

Author:         http://www.jejik.com/articles/2007/02/
                        a_simple_unix_linux_daemon_in_python/www.boxedice.com

License:        http://creativecommons.org/licenses/by-sa/3.0/

Changes:        23rd Jan 2009 (David Mytton <david@boxedice.com>)
                - Replaced hard coded '/dev/null in __init__ with os.devnull
                - Added OS check to conditionally remove code that doesn't
                  work on OS X
                - Added output to console on completion
                - Tidied up formatting
                11th Mar 2009 (David Mytton <david@boxedice.com>)
                - Fixed problem with daemon exiting on Python 2.4
                  (before SystemExit was part of the Exception base)
                13th Aug 2010 (David Mytton <david@boxedice.com>
                - Fixed unhandled exception if PID file is empty
                3rd May 2014 (Adam Marchetti <adamnew123456@gmail.com>)
                - Ported to Python 3
                29th August 2014 (Adam Marchetti <adamnew123456@gmail.com>)
                - Removed PID file handling, which is not used by jobmon
                - Changed sys.exit to os._exit, to avoid unittest catching
                  the SystemExit and doing odd things.
                - Allowed the parent process to stick around, which also aids
                  unit testing
                27th November 2013
                - Added the option to kill the parent, rather than forcing it
                  to stick around for no reason.
'''

# Core modules
import atexit
import os
import sys
import time
import signal

class Daemon(object):
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, stdin=os.devnull, stdout=os.devnull, 
                 stderr=os.devnull, home_dir='.', umask=0o22,
                 kill_parent=True, verbose=1):
        self.kill_parent = kill_parent
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.home_dir = home_dir
        self.verbose = verbose
        self.umask = umask
        self.daemon_alive = True

    def daemonize(self):
        """
        Do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                if self.kill_parent:
                    os._exit(0)
                else:
                    # Let the first parent continue, since that could be running
                    # from the CLI, or running a test, or something
                    return False
        except OSError as e:
            sys.stderr.write(
                "fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            os._exit(1)

        # Decouple from parent environment
        os.chdir(self.home_dir)
        os.setsid()
        os.umask(self.umask)

        # Do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from second parent
                os._exit(0)
        except OSError as e:
            sys.stderr.write(
                "fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            os._exit(1)

        if sys.platform != 'darwin':  # This block breaks on OS X
            # Redirect standard file descriptors
            sys.stdout.flush()
            sys.stderr.flush()
            si = open(self.stdin, 'r')
            so = open(self.stdout, 'a+')
            if self.stderr:
                se = open(self.stderr, 'a+')
            else:
                se = so
            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

        def sigtermhandler(signum, frame):
            self.daemon_alive = False
            signal.signal(signal.SIGTERM, sigtermhandler)
            signal.signal(signal.SIGINT, sigtermhandler)

        return True

    def start(self, *args, **kwargs):
        """
        Start the daemon
        """
        # Start the daemon
        if self.daemonize():
            self.run(*args, **kwargs)

    def stop(self):
        """
        Stop the daemon
        """
        # Try killing the daemon process
        try:
            i = 0
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                i = i + 1
                if i % 10 == 0:
                    os.kill(pid, signal.SIGHUP)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                os._exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon.
        It will be called after the process has been
        daemonized by start() or restart().
        """
