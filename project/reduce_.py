# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown
import time
import collections
import sys

class Reducer(object):
    _tell = ['start', 'shuffle', 'wc', 'cw', 'startCrono', 'stopCrono']
    _ask = [] 		#sincrono
    _ref = []

    def start(self, nmappers):
        self.finished = nmappers
        self.dicc = {}

    def startCrono(self):
        self.timeinit = time.time()

    def stopCrono(self):
        timeend = time.time()
        timeend = timeend - self.timeinit
        print "\n\n\tHa tardado %s segundos" % timeend

    def shuffle(self, dic_map):
        for key in dic_map.keys():
            self.dicc[key] = self.dicc.get(key, 0) + dic_map[key]      #si ya esta en el diccionario le sumamos 1, si no está le podrá el valor de 1

    def wc(self, dic_map):
        self.shuffle(dic_map)
        self.finished -= 1
        if self.finished == 0:
            for key in self.dicc.keys():
                print "%s : %s" % (key, self.dicc[key])
            print "\n\n\tLas palabras repetidas son: (<palabra, #veces>)"
            self.stopCrono()

    def cw(self, dic_map):
        self.shuffle(dic_map)
        self.finished -= 1
        if self.finished == 0:
            print "\n\n\t El archivo tiene en total %s palabras." % self.dicc["Word"]
            self.stopCrono()
