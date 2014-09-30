__author__ = 'coen'
import os
import time

from Queue import Full, Empty

from enhancedmp.enhancedprocessors import StoppableLoggingProcess


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


class FileProcessor(BaseProcessor):
    ''' Base class for file processors
    '''

    def __init__(self, logqueue, message_conn, filename, queue, **kwargs):
        super(FileProcessor, self).__init__(
            logqueue,
            message_conn,
            None,
            **kwargs
        )
        assert (isinstance(filename, str))
        self.filename = filename
        self.f_obj = None
        self.queue = queue

    def afterrun(self):
        self.info("Closing {0}".format(self.filename))
        self.f_obj.close()


class FileReader(FileProcessor):
    ''' Reads from a file, to a queue
    '''

    defaults = {
        "SkipFirstLine": True,
        "ChunkSize": 200,
        "MaxRetries": 5,
        "QueueTimeout": 5
    }

    def __init__(self, logqueue, message_conn, filename, queue, **kwargs):
        super(FileReader, self).__init__(
            logqueue,
            message_conn,
            filename,
            queue,
            **kwargs)

    def beforerun(self):
        self.info("Opening {0} for reading".format(self.filename))
        self.f_obj = open(self.filename, 'r')
        if self.configdict["SkipFirstLine"]:
            self.f_obj.readline()

    def process(self):
        for x in range(self.configdict["ChunkSize"]):
            success = False
            retries = 0
            while not success and retries < self.configdict["MaxRetries"]:
                try:
                    self.queue.put(self.f_obj.readline(),
                                   True,
                                   self.configdict["QueueTimeout"])
                    success = True
                except Full:
                    self.warning("Output queue appears full. Retries: {0}"
                                 .format(retries))
                    retries += 1


class FileWriter(FileProcessor):
    ''' Reads from a queue, writes to a file
    '''

    defaults = {
        "Chunksize": 200,
        "MaxRetries": 5,
        "QueueTimeout": 5,
        "MaxWriteInterval": 10,
        "AppendNewLine": False
    }

    def __init__(self, logqueue, message_conn, filename, queue, **kwargs):
        super(FileWriter, self).__init__(
            logqueue,
            message_conn,
            filename,
            queue,
            **kwargs
        )

    def beforerun(self):
        self.info("Opening {0} for writing".format(self.filename))
        self.f_obj = open(self.filename, 'a')

    def process(self):
        starttime = time.time()
        writeables = []
        while (len(writeables) < self.configdict["ChunkSize"]
               and (time.time() - starttime)
                < self.configdict["MaxWriteInterval"]):
            success = False
            retries = 0
            while not success and retries < self.configdict["MaxRetries"]:
                try:
                    self.queue.get(True, self.configdict["QueueTimeout"])
                    success = True
                except Empty:
                    self.warning("Input Queue appear empty. Retries: {0}"
                                 .format(retries))
                    retries += 1
        self.debug("Writing {0} lines to {1}"
                   .format(len(writeables), self.filename))
        self.writelinestofile(writeables)

    def writelinestofile(self, lines):
        if self.configdict["AppendNewLine"]:
            writeables = [x + '\n' for x in lines]
        else:
            writeables = lines
        self.f_obj.writelines(writeables)
        self.f_obj.flush()

    def afterrun(self):
        self.f_obj.flush()
        os.fsync()
        super(FileWriter, self).afterrun()


class LineProcessor(BaseProcessor):
    defaults = {
        "ReadTimeout": 5,
        "ReadMaxRetries": 5,
        "WriteTimeout": 5,
        "WriteMaxRetries": 5
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












