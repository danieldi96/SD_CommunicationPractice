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
        self.res_cw = 0

    def startCrono(self):
        self.timeinit = time.time()

    def stopCrono(self):
        timeend = time.time()
        timeend = timeend - self.timeinit
        print "\n\n\tHa tardado %s segundos" % str(timeend)

    def shuffle(self, dic_map):
        for key in dic_map.keys():
            if key in self.dicc.keys():
                self.dicc[key] = self.dicc.get(key, 0) + dic_map[key]      #si ya esta en el diccionario le sumamos 1
            else:
                self.dicc[key] = dic_map[key]           #si no esta, anadimos la clave al nuevo diccionario con las ocurrencias de esta en la linea

    def wc(self, dic_map):
        self.shuffle(dic_map)
        self.finished -= 1
        if self.finished == 0:
            print "\n\tLas palabras repetidas son: (<palabra, #veces>)"
            for key in self.dicc.keys():
                line_dic = key+" : "+str(self.dicc[key])
                print line_dic
            print "\n\n\tPuede encontrar las palabras repetidas en: ./files/palabras_repetidas.txt"
            self.stopCrono()

    def cw(self, dic_map):
        self.shuffle(dic_map)
        self.finished -= 1
        if self.finished == 0:
            print "\n\n\t El archivo tiene en total %s palabras." % self.dicc["Paraula"]
            self.stopCrono()
