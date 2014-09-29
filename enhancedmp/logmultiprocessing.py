__author__ = 'coen'

import time
import datetime
import sys

from multiprocessing import Process, Queue
from Queue import Empty, Full

# TODO: more configuration options!

FULLDEBUG = 5
DEBUG = 10
MOREINFO = 20
INFO = 30
WARNING = 40
ERROR = 50
CRITICAL = 60

DEFAULTLOGLEVEL = INFO

LEVELDESCRIPTION = {
    5: "FULLDEBUG",
    10: "DEBUG",
    20: "MOREINFO",
    30: "INFO",
    40: "WARNING",
    50: "ERROR",
    60: "CRITICAL"
}


def get_level_description(level):
    try:
        return LEVELDESCRIPTION[level]
    except Exception as e:
        return "UNKNOWNLEVEL"


def setup(logfile):
    logqueue = Queue()

    logger = ProcessLogger(logqueue, logfile)

    return logqueue, logger


class LogMessage(object):
    ''' LogMessage sent by multiprocessor
        Note that comparison operators are defined in order to
        sort log messages by their timestamp.
        message1 < message2 is true iff message1.timestamp < message2.timestamp
    '''

    def __init__(self, level, message, origin="unknown"):
        self.level = level
        self.message = message
        self.origin = origin
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

    def age(self):
        return time.time() - self.timestamp

    def __str__(self):
        return "{0}.{4} - {1}[{3}]: {2}".format(
            datetime.datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            self.get_level_description(),
            self.message, self.origin,
            str(self.timestamp - int(self.timestamp))[2:]
        )


class Logger(object):
    def __init__(self, name=None):
        if name is not None:
            assert (isinstance(name, str))
            self.name = name
        else:
            self.name = "NameNotSet"

    def log(self, message, level=DEFAULTLOGLEVEL):
        raise NotImplementedError(
            "log(self, message, level) has no implementation in logger {0}, class {1}"
            .format(self.name, self.__class__)
        )

    def buildmessageandlog(self, message, level):
        assert (isinstance(message, str))
        assert (isinstance(level, int))
        self.log(LogMessage(level, message, self.name))

    def fulldebug(self, message):
        self.buildmessageandlog(message, FULLDEBUG)

    def debug(self, message):
        self.buildmessageandlog(message, DEBUG)

    def moreinfo(self, message):
        self.buildmessageandlog(message, MOREINFO)

    def info(self, message):
        self.buildmessageandlog(message, INFO)

    def warning(self, message):
        self.buildmessageandlog(message, WARNING)

    def error(self, message):
        self.buildmessageandlog(message, ERROR)

    def critical(self, message):
        self.buildmessageandlog(message, CRITICAL)


class LoggingProcess(Process, Logger):
    ''' Multiprocessor with logging functionality
    '''

    def __init__(self, log_queue, name=None):
        Process.__init__(self)
        Logger.__init__(self, name)
        self.log_queue = log_queue

    def log(self, message):
        retries = 0
        success = False
        # TODO: make retries a parameter
        while retries < 5 and not success:
            try:
                self.log_queue.put(message)
                success = True
            except Full:
                retries += 1

        if not success:
            raise Exception("Logging queue overflow")
        elif retries > 0:
            self.warning("Had to retry {0} times to write to log"
                         .format(retries))

class ProcessLogger(Process, Logger):
    ''' Should be instantiated by main process, collects and sorts log
        messages
    '''

    def __init__(self, log_queue, logfile=None):
        Process.__init__(self)
        Logger.__init__(self, "ProcessLogger")

        if logfile is None:
            self.logfile = sys.stdout
        else:
            self.logfile = logfile
        self.log_queue = log_queue
        self.messagestack = []

    def run(self):
        ''' Probably good to override this method, it won't stop... Ever...
        '''
        while True:
            self.processlogs()

    def processlogs(self):
        ''' Get log messages, sort them and write to logfile
        '''
        self.getmessages(5)
        self.sortandwrite()

    def getmessages(self, timeout=-1):
        '''
        :param timeout: negative timeout means: get full queue
        :return:
        '''
        queue_empty = False
        start = time.time()
        # TODO: make max message a parameter and seconds as well
        while (len(self.messagestack) < 10000
               and not queue_empty
               and ((time.time() - start < timeout) or timeout < 0)):
            try:
                message = self.log_queue.get(True, 2
                self.messagestack.append(message)
            except Empty:
                queue_empty = True
        if queue_empty:
            self.fulldebug("Emptied queue, stack size: {0}"
                           .format(len(self.messagestack)))
        else:
            self.fulldebug("Pulled messages from queue, stack size: {0}"
                           .format(len(self.messagestack)))

    def sortandwrite(self, split=0.5):
        self.messagestack.sort()
        # TODO: make splitpoint a parameter
        with open(self.logfile, 'a') as f_obj:
            splitpoint = int(round(0.5 * len(self.messagestack)))

            writables = [str(x) + "\n" for x in self.messagestack[:splitpoint]]
            self.messagestack = self.messagestack[splitpoint:]
            f_obj.writelines(writables)

    def log(self, message):
        assert isinstance(message, LogMessage)
        self.messagestack.append(message)
