__author__ = 'coen'

import time
import datetime
import sys

from multiprocessing import Process, JoinableQueue
from Queue import Empty, Full

FULLDEBUG = 5
DEBUG = 10
MOREINFO = 20
INFO = 30
WARNING = 40
ERROR = 50
CRITICAL = 60

LEVELDESCRIPTION = {
    5:"FULLDEBUG",
    10:"DEBUG",
    20:"MOREINFO",
    30:"INFO",
    40:"WARNING",
    50:"ERROR",
    60:"CRITICAL"
}

def get_level_description(level):
    try:
        return LEVELDESCRIPTION[level]
    except Exception as e:
        return "UNKNOWNLEVEL"

class LogMessage(object):
    ''' LogMessage sent by multiprocessor
        Note that comparison operators are defined in order to
        sort log messages by their timestamp.
        message1 < message2 is true iff message1.timestamp < message2.timestamp
    '''

    def __init__(self, level, message):
        self.level = level
        self.message = message
        self.timestamp = time.time()

    def get_level_description(self):
        return get_level_description(self.level)

    def __eq__(self, other):
        return self.timestamp == other.timestamp

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __gt__(self, other):
        return self.timestamp > other.timestamp

    def __le__(self, other):
        return (self < other) or (self == other)

    def __ge__(self, other):
        return (self > other) or (self == other)

    def __len__(self):
        return len(self.message)

    def __str__(self):
        return "{0} - {1}: {2}".format(
            datetime.datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            self.get_level_description(),
            self.message
        )

class ProcessLogger(Process):
    ''' Should be instantiated by main process, collects and sorts log
        messages
    '''

    def __init__(self, log_queue, logfile=None):
        super(ProcessLogger, self).__init__()
        if logfile is None:
            self.logfile = sys.stdout
        else:
            self.logfile = logfile
        self.log_queue = log_queue
        self.messages = []

    def run(self):
        ''' Get log messages, sort them and write to logfile
        '''
        with open(self.logfile, 'w') as f_obj:





    def get_log_messages(self):
        ''' Gets log messages from queue until newest is at least 3 secs old
            Returns them sorted by timestamp
        '''






class LoggingProcess(Process):
    ''' Multiprocessor with logging functionality
    '''

    def __init__(self, log_queue):
        super(LoggingProcess, self).__init__()
        self.log_queue = log_queue

    def debug(self, message):
        self.log_queue.put(LogMessage(DEBUG, message))

    def info(self, message):
        self.
