JobMon
======

JobMon is a job monitoring system, which is capable of:

- Monitoring child processes; generally, this means that JobMon can start
  a process for you, allow you to query its status (if it is running or not),
  kill the process, and receive notifications when the process is started or 
  it dies.
- Configuring the environment that child processes run it. Currently, this
  is a useful but small list of things that can be configured. These include:

  - Telling the child processes what files it should hook its standard IO
    streams up to.
  - Setting up environment variables for the child process.
  - Setting the working directory of the child process.
  - What signal is sent to the child process to kill it.

- Handling commands, and sending responses, over UNIX sockets. The protocol
  (which uses JSON) is covered later in this document.
- Dispatching events over UNIX sockets using a similar protocol to that
  employed by commands and responses.

JobMon does not yet, however, contain built-in ways to autostart processes,
restart processes when they die, etc. However, JobMon is powerful enough to
implement these constructs on top of the provided command set.

JobMon is, conceptually, two pieces - an API (which can be used from Python
code) and a command line utility. The pieces common to both (which is
mostly configuration) are described first. The API is documented by the
modules themselves, and will be elided here.

Configuration
-------------

Configuring JobMon is done via a JSON-formatted configuration file, which
may "include" any number of other JSON-formatted configuration files. The
main file is called the "master", while the files it includes are called
"job files". These have slightly different formats.

Master Configuration
~~~~~~~~~~~~~~~~~~~~

The master configuration can contain two sections - one which configures the
JobMon supervisor itself, and another which specifies jobs. The configuration
format for jobs is discussed in the next section, since that is common to
both the master configuration as well as job files.

Here is an example of a master configuration::

    {
        "supervisor":
        {
            "working-dir": ".",
            "control-dir": "$TMP/supervisor",
            "include-dirs": [
                "jobs/*.json"
            ],
            "log-file": "$TMP/supervisor.log",
            "log-level": "WARNING"
        },
        "jobs":
        {
            ...
        }
    }

Each part of the supervisor configuration is described below:

- ``working-dir`` sets the working directory for the supervisor daemon. This
  can be useful if the paths used in the configuration file are relative
  paths. The default is not to change the working directory (i.e. use ``.``).
- ``control-dir`` sets the location where the UNIX sockets for events and
  commands are located. The default is ``.`` (the current directory).
- ``include-dirs`` is a list of globs, each of which should reference a list
  of job files to include. The default is that no files are included.
- ``log-file`` is the path to the daemon's logs. Note that file is appended
  to, so no previous log data is lost on subsequent uses (but the file can
  also grow to large sizes, depending upon what is logged). The default is
  ``/dev/null``.
- ``log-level`` indicates what kinds of messages are logged. The following
  constants are used in this field (note that this field is 
  *case-insensitive*):

  - ``DEBUG`` prints out the most messages, and is really only useful for
    development.
  - ``INFO`` prints out messages which are generally not of interest outside 
    of development (although fewer are printed than when ``DEBUG`` is used).
  - ``WARN`` (also, ``WARNING``) print out messages which indicate some kind
    of error in a configuration file or the code which, while not fatal, is
    generally worth paying attention to. This is the default logging level.
  - ``ERROR`` prints out serious error messages.
  - ``CRITICAL`` prints out messages which are extremely important.

Note that ``working-dir``, ``control-dir``, ``include-dirs`` and ``log-file``
will expand shell variables using the traditional ``$NAME`` syntax. Note
that ``$$`` escapes into a single ``$``.

Job Files
~~~~~~~~~

Job files are a subset of the full master configuration file. The section
elided in the original example is where job information would be stored,
that is, inside the hash called "jobs".

When using a jobs file (that is included by the master), the top-level
"jobs" can be elided. A sample jobs file follows::

    {
        "true-job":
        {
            "command": "/bin/true",
            "stdin": "$DEVNULL",
            "stdout": "$DEVNULL",
            "stderr": "$DEVNULL",
            "env": {
                "HOME": "/home/bob",
            },
            "working-dir": "/home/bob",
            "signal": "SIGSTOP",
            "autostart": false
        }
    }

- ``"true-job"`` is the name of the job. These names can include any character,
  but must be globally unique (that is, neither the master nor any other files
  included by the master can use this same name).
- The ``command`` option (which is *mandatory*) indicates the command to
  launch. Note that this command can use syntax supported by ``/bin/sh``.
- ``stdin``, ``stdout``, and ``stderr`` give a filename which is hooked up to
  the named standard IO stream. Each of these has a default of ``/dev/null``.
  Note that ``stdout`` and ``stderr`` are appended to, not cleared.
- ``env`` is a hash which gives a set of environment variables to pass to the
  child process, and their values. Note that all of the daemon's environment
  variables are passed in as well, in addition to these variables, but the
  variables in the configuration file take precedence.
- ``working-dir`` sets the working directory of the child - the default is ``.``
- ``signal`` sets the signal that is sent to the child process when it is
  stopped. The values allowed in this (case-insensitive) field can be found
  by running ``kill -l`` on your system - however, the preceding ``SIG`` is
  *required*. The default signal is ``SIGTERM``.
- ``autostart`` dictates whether or not the job should be started
  automatically by the daemon (the default is that the job is *not* started
  automatically).

Note that the ``stdin``, ``stdout``, ``stderr``, and ``working-dir`` fields do
environment substitution in the same way as in the supervisor configuration
discussed above.

The Command Line Tool
---------------------

The command line tool, called ``jobmon``, is designed to give a convenient
interface to the capabilities of JobMon. The tool's internal documentation
can be viewed by calling ``jobmon help``.

The first thing to remember about the command line tool is the special
environment variable called ``$JOBMON_CONTROL_DIR``. This variable *must* be
set if you are using any subcommand which is not ``help`` or ``daemon``; this
is because it is used to store the control directory (where the UNIX sockets 
are stored). The initial value can be obtained as follows::

    # When starting the daemon...
    $ export JOBMON_CONTROL_DIR=`jobmon daemon CONFIG`

As a general rule, note that any command (other than ``status``) will return
0 on success and nonzero on failure (and will also print a message on
standard error).  ``status`` is special in this regard - if it encounters an
error, it returns a *negative* status code; if the job that it queries is
running, the it returns a 0, while if the job it queries is stopped, it
returns a positive status code.

``jobmon list-jobs`` and ``jobmon listen`` share a common output format. For
example, consider a JobMon instance with two jobs, *Job A* which is running and
*Job B* which is stopped. ``jobmon list-jobs`` should print::

    RUNNING Job A
    STOPPED Job B

Let's say that *Job A* was started, then *Job B* was started, and then *Job B*
stopped. ``jobmon listen`` might produce the following event stream::

    RUNNING Job A
    RUNNNIG Job B
    STOPPED Job B

Installation
------------

Simply run ``python3 setup.py install`` to install this package. Note that
Python 3 is required (I have not tested this on any version but 3.4, and
thus this code probably requires Python >=3.4).

Unit Tests
----------

JobMon is currently tested, although not completely (and the tests could
probably be a bit neater too). The easiest way to run a single test is to
do::

    $ ./run-tests.sh [TEST]
    # For example, to run the configuration handler test
    $ ./run-tests.sh test_config
    # Or, to run the entire test suite
    $ ./run-tests.sh

where ``TEST`` is the name of a file (without the ``.py`` extension) of the
test to run under ``test/tests``.

Misc. Info
----------

Written by Adam Marchetti <adamnew123456@gmail.com>, and released under the
2-clause BSD license.

The file ``jobmon/daemon.py`` was written by David Mytton <david@boxedice.com>
and released under a Creative Commons BY-SA 3.0 license. Modifications were
made by Adam Marchetti <adamnew123456@gmail.com>. The original version can
be found at the link provided in the source file itself.
