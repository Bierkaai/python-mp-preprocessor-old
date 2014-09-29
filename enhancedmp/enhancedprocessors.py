__author__ = 'coen'

from multiprocessing import Pipe
from logmultiprocessing import *
from stoppablemultiprocessing import StoppableProcess


def setuplogging(logfile, loglevel):
    assert(isinstance(logfile, str))
    assert(isinstance(loglevel, int))

    logqueue = Queue()
    message_conn, to_main = Pipe()
    logger = StoppableProcessLogger(logqueue, message_conn, logfile)

    return logqueue, logger, to_main


class StoppableLoggingProcess(LoggingProcess, StoppableProcess):

    def __init__(self, logqueue, message_conn, name):
        LoggingProcess.__init__(self, logqueue, name)
        StoppableProcess.__init__(self, message_conn)

    def run(self):
        self.fulldebug("Starting process (beforerun method)")
        self.beforerun()
        self.fulldebug("Entering main loop (process method)")
        while self.stayAlive:
            self.process()
            self.checkmessages()
        self.fulldebug("Finalizing process (afterrun method)")
        self.afterrun()

    def checkmessages(self):
        if self.messages.poll():
            m = self.messages.recv()
            self.fulldebug("Received message: {0}".format(m))
            self.processmessage(m)

class StoppableProcessLogger(ProcessLogger, StoppableProcess):

    def __init__(self, logqueue, message_conn, logfile):
        ProcessLogger.__init__(self, logqueue, logfile)
        StoppableProcess.__init__(self, message_conn)

    def run(self):
        self.fulldebug("Entering main loop")
        while self.stayAlive:
            self.processlogs()
            self.checkmessages()
        self.fulldebug("Exiting main loop, trying to empty queue")
        self.getmessages()
        self.sortandwrite(1)

    def checkmessages(self):
        if self.messages.poll():
            m = self.messages.recv()
            self.fulldebug("Received message: {0}".format(m))
            self.processmessage(m)