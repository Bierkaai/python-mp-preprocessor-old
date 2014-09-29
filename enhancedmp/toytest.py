__author__ = 'coen'

import sys
import time
import random


from logmultiprocessing import *
from stoppablemultiprocessing import StoppableProcess


class Randomlogger(LoggingProcess):

    def __init__(self, logqueue, name):
        super(Randomlogger, self).__init__(logqueue, name)
        random.seed()

    def run(self):
        while True:
            time.sleep(random.randint(0,5))
            self.debug("Slept a while. Woke up")
            time.sleep(random.randint(0,5))
            self.debug("Going back to sleep...")


if __name__ == "__main__":
    logqueue, logger = setup("logfile.log")

    sleepers = []
    for x in range(300):
        sleeper = Randomlogger(logqueue, "Sleeper {0}".format(x))
        sleepers.append(sleeper)

    logqueue.put(LogMessage(DEBUG, "TEST"))

    logger.start()
    time.sleep(5)

    for s in sleepers:
        s.start()

