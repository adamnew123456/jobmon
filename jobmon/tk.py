"""
Monitors an instance of jobmon, showing the status of its jobs, and allowing jobs
to be started and stopped.
"""
from collections import namedtuple
import queue
import sys
import threading
import time
import tkinter, tkinter.ttk as ttk

from jobmon import protocol, transport
from jobmon.protocol import EVENT_STARTJOB, EVENT_STOPJOB, EVENT_RESTARTJOB

def add_tab_with_frame(notebook, **kw):
    """
    Adds a tab to the notebook, returning the frame which can be used to 
    populate that tab.
    """
    frame = tkinter.Frame(notebook)
    notebook.add(frame, **kw)
    return frame

def queue_events(event_stream, output_queue):
    """
    Waits for events from the event stream, and outputs them onto the queue.
    """
    while True:
        event = event_stream.next_event()
        output_queue.put(event)

class EventDispatcher:
    """
    This class waits for jobmon events on a queue, and then sends them out to
    handlers which are registered to callback functions.
    """
    def __init__(self, event_queue):
        self.registry = {}
        self.event_queue = event_queue

    def register(self, callback, job_name=None, event_type=None):
        """
        Registers a callback, which is run under a given rule. For example:

            register(x) # Runs on every event
            register(x, job_name=X) # Runs for every event on the X job
            register(x, event_type=Y) # Runs for the Y event on any job
            register(x, job_name=X, event_type=Y) # Runs only when the job
                                                  # is X *and* the event is
                                                  # Y
        """
        self.registry[callback] = (job_name, event_type)

    def run_updates(self):
        """
        Dispatches on any events which have come through the queue since
        this method was last run.
        """
        while not self.event_queue.empty():
            event = self.event_queue.get()
            self.dispatch(event.job_name, event.event_code)

    def dispatch(self, job, event_type):
        """
        Calls all the matching functions for a given job and event.
        """
        for callback, pattern in self.registry.items():
            pattern_job, pattern_event = pattern

            if ((pattern_job == job or pattern_job is None) and
                    (pattern_event == event_type or pattern_event is None)):
                callback(job, event_type)

class EventLog:
    """
    The event log displays all events which have occurred since the application
    was started.
    """
    EVENT_STRINGS = {
        protocol.EVENT_STARTJOB: 'Started',
        protocol.EVENT_STOPJOB: 'Stopped',
        protocol.EVENT_RESTARTJOB: 'Restarted',
    }

    def __init__(self, parent, dispatcher):
        self.log = tkinter.Text(parent)
        dispatcher.register(self.log_event)

    def format_time(self):
        """
        Returns a formatted time string representing the current time.
        """
        return time.strftime('[%d %b %Y: %H:%M:%S] ')

    def log_message(self, message, *args, **kwargs):
        """
        Logs a message, formatting it before writing it to the log using
        str.format
        """
        self.log.insert('end', 
            self.format_time() + message.format(*args, **kwargs) + '\n')

    def log_event(self, job_name, event_type):
        """
        Logs an event which occurred on a particular job.
        """
        self.log.insert('end',
            self.format_time() + '{event}: {job}\n'.format(
                event=self.EVENT_STRINGS[event_type], job=job_name))

    def pack(self, **options):
        """
        Uses the pack geometry manager to display the widget. 
        See tinter.Widget.pack().
        """
        self.log.pack(**options)

class JobTracker:
    """
    This displays the status of all jobs, by changing the color of the job's
    name. It also allows the user to start and stop the selected job.
    """
    def __init__(self, parent, commands, dispatcher):
        self.commands = commands
        self.frame = tkinter.Frame(parent)
        self.job_list = tkinter.Listbox(self.frame)
        self.job_action = tkinter.Button(self.frame, text='Start/Stop', command=self.do_action,
            state='disabled')
        self.job_states = {}

        self.job_list.bind('<<ListboxSelect>>', self.on_selection_changed)
        dispatcher.register(self.on_job_state_change)
        self.load_jobs()

    def load_jobs(self):
        """
        Loads all the jobs on the server, and gets their current status. Note
        that this should only be run on startup.
        """
        self.job_states = self.commands.get_jobs()
        self.jobs_index = []
        for job, is_running in sorted(self.job_states.items()):
            self.jobs_index.append(job)
            self.job_list.insert('end', job)
            if is_running:
                self.job_list.itemconfig('end',
                    foreground='green', selectforeground='green')
            else:
                self.job_list.itemconfig('end', 
                    foreground='black', selectforeground='black')

    def on_selection_changed(self, event):
        """
        Updates the state of the action button, which can be either:

        - 'Start' for when the selected job is stopped
        - 'Stop' for when the selected job is running
        - Disabled for when there is no selected job.
        """
        selection = self.job_list.curselection()
        if not selection:
            self.job_action.config(state='disabled')
            return

        selected_job_name = self.job_list.get(selection[0])
        job_status = self.job_states[selected_job_name]
        if job_status:
            self.job_action.config(state='active', text='Stop')
        else:
            self.job_action.config(state='active', text='Start')

    def on_job_state_change(self, job, event):
        """
        Updates the state of the given job, in response to the given event. This
        also updates the color of the job on the UI.
        """
        self.job_states[job] = (
            event in (protocol.EVENT_STARTJOB, protocol.EVENT_RESTARTJOB))

        job_index = self.jobs_index.index(job)
        self.job_list.itemconfig(job_index, 
            foreground='green' if self.job_states[job] else 'black',
            selectforeground='green' if self.job_states[job] else 'black')

        # If the currently selected item changes its state, then the rest of
        # the UI needs to know.
        self.on_selection_changed(None)

    def do_action(self):
        """
        Either starts the currently selected job (when it is not running), or 
        stops the currently selected job (when it is).
        """
        selection = self.job_list.curselection()[0]
        selected_job_name = self.job_list.get(selection)

        if self.job_states[selected_job_name]:
            self.commands.stop_job(selected_job_name)
        else:
            self.commands.start_job(selected_job_name)

    def pack(self, **options):
        """
        Uses the pack geometry manager to display the widget. 
        See tinter.Widget.pack().
        """
        self.job_list.pack(expand=True, fill='both')
        self.job_action.pack(side='bottom')
        self.frame.pack(**options)

def main():
    try:
        location = sys.argv[1]

        event_stream = transport.EventStream(location)
        command_pipe = transport.CommandPipe(location)
    except IndexError:
        print(sys.argv[0], '<SOCKET-DIR>', file=sys.stderr)
        return 1
    except IOError as err:
        print(err, file=sys.stderr)
        print('(Are you sure you gave the correct directory?', file=sys.stderr)
        return 1

    # Kick off the event thread, so that way we can start processing ASAP (we
    # want to catch as many events as possible, even if we can't process them
    # immediately)
    event_queue = queue.Queue()
    event_thread = threading.Thread(target=queue_events, args=(event_stream, event_queue))
    event_thread.setDaemon(True)
    event_thread.start()

    dispatcher = EventDispatcher(event_queue)

    win = tkinter.Tk()
    tabs = ttk.Notebook()

    event_log_frame = add_tab_with_frame(tabs, text='Event Log')
    event_log = EventLog(event_log_frame, dispatcher)
    tracker_frame = add_tab_with_frame(tabs, text='Jobs')
    tracker = JobTracker(tracker_frame, command_pipe, dispatcher)

    tabs.pack(expand=True, fill='both')
    event_log.pack(expand=True, fill='both')
    tracker.pack(expand=True, fill='both')

    def update_events():
        dispatcher.run_updates()
        win.after(500, update_events)

    event_log.log_message('Started application')
    win.after(500, update_events)
    win.mainloop()

if __name__ == '__main__':
    main()
