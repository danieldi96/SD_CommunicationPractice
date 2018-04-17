# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown, serve_forever
import time
from collections import Counter
import sys
global remote_master

class Reducer(object):
    _tell = ['wc', 'cw', 'startCrono', 'stopCrono']
    _ask = ['start']
    _ref = ['start']

    def start(self, nmappers, reg):
        """
        Inicializes the parameters of the class.
        :param nmappers: number of mappers.
        """
        self.finished = nmappers
        self.registry = reg
        self.dicc = Counter()
        self.timeinit = 0
        self.dicc.update(Word=0)

    def startCrono(self):
        """
        Start the stopwatch.
        """
        self.timeinit = time.time()

    def stopCrono(self):
        """
        Stop the stopwatch.
        """
        timeend = time.time()
        self.timeinit = timeend - self.timeinit

    def wc(self, dic_map):
        """
        Shuffle the dictionaries to mix the equal words together.
        :param dic_map: the dictionary of the mapper.
        """
        self.dicc.update(dic_map)
        self.finished -= 1
        if self.finished == 0:
            self.stopCrono()
            self.registry.results("WC",self.dicc,self.timeinit)

    def cw(self, dic_map):
        """
        Call "Shuffle", substract 1 to the number of mappers (finished) to know if it's
        finished and print the words with the number of repetitions of these.
        :param dic_map: the dictionary of the mapper.
        """
        self.dicc.update(dic_map)
        self.finished -= 1
        if self.finished == 0:
            self.stopCrono()
            self.registry.results("CW",self.dicc,self.timeinit)

if __name__ == "__main__":
    global remote_master
    set_context()
    try:
        if len(sys.argv) != 3:
            raise IndexError
        ip = str(sys.argv[1])
        ip_sv = str(sys.argv[2])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython reduce.py [ip_reducer] [ip_master]\n\n\t* si las ip's son 'localhost' = 127.0.0.1\n"
        shutdown()
    finally:
        if ip == "localhost":
            ip = "127.0.0.1"
        if ip_sv == "localhost":
            ip_sv = "127.0.0.1"
        host = create_host('http://%s:1700/' % (str(ip)))
        reduce = host.spawn("Reducer", "reduce/Reducer")
        print reduce
        print "\n\tCargando...\n"
        remote_master = host.lookup_url("http://%s:1500/regis"%ip_sv, 'Registry', 'master')
        remote_master.bind("Reducer")
        serve_forever()
