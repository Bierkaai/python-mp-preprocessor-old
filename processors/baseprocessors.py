
__author__ = 'coen'

from Queue import Full, Empty
from enhancedmp.enhancedprocessors import setuplogging, StoppableLoggingProcess
from enhancedmp.stoppablemultiprocessing import STOP, Message


class Configurable(object):
    defaults = {}

    def config(self, **kwargs):
        self.configdict = self.defaults
        for (key, value) in self.defaults.items:
            if key in kwargs:
                self.configdict[key] = kwargs[key]

class BaseProcessor(StoppableLoggingProcess, Configurable):

    def __init__(self, logqueue, message_conn, name=None, **kwargs):
        super(BaseProcessor, self).__init__(
            logqueue,
            message_conn,
            name
        )
        self.config(**kwargs)


class FileReader(BaseProcessor):

    defaults = {
        "SkipFirstLine":True,
        "ChunkSize":200,
        "MaxRetries":5
    }

    def __init__(self, logqueue, message_conn, filename, output_queue, **kwargs):
        super(FileReader, self).__init__(
            logqueue,
            message_conn,
            "FileReader reading: {0}".format(filename),
            **kwargs)
        self.fulldebug("Config: {0}".format(self.configdict))

        assert(isinstance(filename, str))
        self.filename = filename
        self.f_obj = None
        self.output_queue = output_queue

    def beforerun(self):
        self.info("Opening {0} for reading". format(self.filename))
        self.f_obj = open(self.filename, 'r')
        if self.configdict["SkipFirstLine"]:
            self.f_obj.readline()

    def process(self):
        for x in range(self.configdict["ChunkSize"]):
            success = False
            retries = 0
            while not success and retries < self.configdict["MaxRetries"]:
                try:
                    self.output_queue.put(self.f_obj.readline())
                    success = True
                except Full:
                    self.warning("Output queue appears full. Retries: {0}"
                                 .format(retries))
                    retries += 1

    def afterrun(self):
        self.info("Closing {0}".format(self.filename))
        self.f_obj.close()


class LineProcessor(BaseProcessor):

    defaults = {
        "ReadTimeout":5,
        "ReadMaxRetries":5,
        "WriteTimeout":5,
        "WriteMaxRetries":5
    }

    def __init__(self, logqueue, message_conn, input_queue, output_queue, **kwargs):
        super(LineProcessor, self).__init__(
            logqueue,
            message_conn,
            "LineProcessor",
            **kwargs
        )
        self.input_queue = input_queue
        self.output_queue = output_queue

    def process(self):
        read_retries = 0
        read_success = False
        write_retries = 0
        write_success = False
        while read_retries < self.configdict["ReadMaxRetries"] and not read_success:
            try:
                line = self.input_queue.get(True, self.configdict["ReadTimeout"])
                read_success = True
            except Empty:
                read_retries += 1
        if not read_success:
            self.error("Reading from input queue failed, assuming too many workers, dying")
            self.stayAlive = False
        else:
            if read_retries > 0:
                self.warning("Reading from input success after {0} retries".format(read_retries))
            result = self.processLine(line)
            while write_retries < self.configdict["WriteMaxRetries"] and not write_success:
                try:
                    self.output_queue.put(result)
                    write_success = True
                except Full:
                    write_retries += 1
            if not write_success:
                # TODO: backup queue implementation
                self.error("Writing to output queue failed")
            else:
                if write_retries > 0:
                    self.warning("Writing to output success after {0} retries".format(write_retries))
            self.input_queue.task_done()

    def processLine(self, line):
        '''
        This method should be overridden in subclass
        :param line: Input line read from queue
        :return: Processed line as string
        '''
        pass












