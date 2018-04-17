# -*- coding = utf-8 -*-
from pyactor.context import sleep
import os, sys

if __name__ == "__main__" :
    try:
        mappers_total = int(sys.argv[1])
        ip = str(sys.argv[2])
    except IndexError, SyntaxError:
        print "\n\tpython spawn_mappers.py [num_mappers_total] [ip_servidor_archivos*] \n\n\t* si ip_servidor_archivos es 'localhost' = 127.0.0.1\n"
    finally:
        print "\nEjecutando..."
        for i in range(0, mappers_total):
            os.system("gnome-terminal -e 'bash -c \"python mapper.py %s %s; exec bash\"'" % (i, ip))
            #os.system("python mapper.py %s %s &" % (i, ip))
        os.system("gnome-terminal -e 'bash -c \"python reduce.py %s; exec bash\"'" % ip)
