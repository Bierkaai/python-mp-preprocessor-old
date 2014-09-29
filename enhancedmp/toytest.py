__author__ = 'coen'

import sys
import time
import random

from enhancedprocessors import *

from multiprocessing import Pipe
from stoppablemultiprocessing import Message, STOP


class RandomLogger(StoppableLoggingProcess):

    def __init__(self, logqueue, message_conn, name):
        super(RandomLogger, self).__init__(logqueue, message_conn, name)
        random.seed()

    def process(self):
        time.sleep(random.randint(0,5))
        self.debug("Slept a while. Woke up")
        time.sleep(random.randint(0,5))
        self.debug("Going back to sleep...")


if __name__ == "__main__":
    logqueue, logger, logger_connection = setuplogging("logfile.log", FULLDEBUG)

    sleepers = []
    connections = []
    for x in range(4):
        to_process, to_me = Pipe()
        connections.append(to_me)
        sleeper = RandomLogger(logqueue, to_process, "Sleeper {0}".format(x))
        sleepers.append(sleeper)

    logqueue.put(LogMessage(DEBUG, "TEST"))

    logger.start()
    time.sleep(5)

    for s in sleepers:
        s.start()

    time.sleep(20)

    for c in connections:
        c.send(Message(STOP))

    for s in sleepers:
        s.join()

    time.sleep(10)

    logger_connection.send(Message(STOP))

    logger.join()



