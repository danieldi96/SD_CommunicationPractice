# -*- coding = utf-8 -*-
import os, sys

if __name__ == "__main__" :
    try:
        mappers_total = int(sys.argv[1])
        ip = str(sys.argv[2])
        program = str(sys.argv[3])
    except IndexError, SyntaxError:
        print "\n\tpython spawn_mappers.py [num_mappers_total] [ip_servidor_archivos*] [WC o CW]\n\n\t* si ip_servidor_archivos es 'localhost' = 127.0.0.1\n"
    finally:
        for i in range(0, mappers_total):
            print "MAPPER "+str(i)
            os.system("python mapper.py %s %s %s" % (str(i), ip, program))
            print "python mapper.py %s %s %s" % (str(i), ip, program)
