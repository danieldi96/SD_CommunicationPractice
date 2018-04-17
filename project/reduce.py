# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown
import time
from collections import Counter
import sys

class Reducer(object):
    _tell = ['start', 'shuffle', 'wc', 'cw', 'startCrono', 'stopCrono']
    _ask = []
    _ref = []

    def start(self, nmappers):
        self.finished = nmappers
        self.dicc = Counter()
        self.timeinit = 0
        self.dicc.update(Word=0)

    def startCrono(self):
        self.timeinit = time.time()

    def stopCrono(self):
        timeend = time.time()
        self.timeinit = timeend - self.timeinit
        print "\n\n\tHa tardado %s segundos" % self.timeinit

    def wc(self, dic_map):
        self.dicc.update(dic_map)
        self.finished -= 1
        if self.finished == 0:
            for key in self.dicc.keys():
                print "%s : %s" % (key, self.dicc[key])
            self.stopCrono()
            print "\n\n\tLas palabras repetidas son: (<palabra, #veces>)"

    def cw(self, dic_map):
        self.dicc.update(dic_map)
        self.finished -= 1
        if self.finished == 0:
            self.stopCrono()
            print "\n\n\tEl archivo tiene en total %s palabras." % self.dicc["Word"]
