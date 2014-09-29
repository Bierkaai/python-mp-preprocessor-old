
__author__ = 'coen'

from enhancedmp.enhancedprocessors import setuplogging, StoppableLoggingProcess
from enhancedmp.stoppablemultiprocessing import STOP, Message


class FileReader(StoppableLoggingProcess):

    def __init__(self, logqueue, message_conn, filename):
        super(FileReader, self).__init__(
            logqueue,
            message_conn,
            "FileReader reading: {0}".format(filename))
        self.filename = filename




