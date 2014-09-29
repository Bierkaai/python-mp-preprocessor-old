__author__ = 'coen'


from multiprocessing import Process


STOP = 0

class message(object):

    def __init__(self, message=STOP):
        self.message = message

class StoppableProcess(Process):

    def __init__(self, message_connection):
        Process.__init__(self)
        self.messages = message_connection

    def run(self):
        self.beforerun()
        self.stayAlive = True
        while self.stayAlive:
            self.process()
            self.checkmessages()
        self.afterrun()

    def beforerun(self):
        pass

    def process(self):
        pass

    def afterrun(self):
        pass

    def checkmessages(self):
        if self.messages.poll():
            m = self.messages.recv()
            self.processmessage(m)

    def processmessage(self, m):
        if m.message == STOP:
            self.stayAlive = False