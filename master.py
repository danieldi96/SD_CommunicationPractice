# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever
import io
import mapper as mp
global dictionary
class Master(object):
    _tell = []
    _ask = ['readFile']

    def readFile(self, slave):
        i = 0
        file = io.open("fichero.txt", "r", encoding="utf-8")
        for line in iter(lambda: file.readline(), ""):
            dictionary[i] = slave.map(i, line)
            i += 1
        file.close()


if __name__ == "__main__":
    dictionary = {}
    set_context()
    host = create_host("http://127.0.0.1:1551")
    master1 = host.spawn('Master1', Master)
    for slaves in
        slave1 = host.spawn('Slave1', 'mapper/Mapper')
        master1.readFile(slave1)
    print "\n==================================\n"
    print dictionary
    serve_forever()
