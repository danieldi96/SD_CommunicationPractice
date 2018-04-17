# -*- coding = utf-8 -*-
from pyactor.context import sleep
import os, sys

if __name__ == "__main__" :
    try:
        mappers_total = int(sys.argv[1])
        ip = str(sys.argv[2])
        ip_sv = str(sys.argv[3])
    except IndexError, SyntaxError:
        print "\n\tpython spawn_mappers.py [num_mappers_total] [ip_reducer] [ip_master] \n\n\t* si las ip's son 'localhost' = 127.0.0.1\n"
    finally:
        print "\nEjecutando..."
        for i in range(0, mappers_total):
            os.system("gnome-terminal -e 'bash -c \"python mapper.py %s %s %s; exec bash\"'" % (i, ip, ip_sv))
            #os.system("python mapper.py %s %s &" % (i, ip))
        os.system("gnome-terminal -e 'bash -c \"python reduce.py %s %s; exec bash\"'" % (ip, ip_sv))
