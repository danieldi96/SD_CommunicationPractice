# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever
import io
import os
import sys
import commands
global dictionary, url_host, nombre_fichero, num_slaves

# Global variables
url_host = "http://127.0.0.1:1500/"
ip = "10.110.178.237:8000"
nombre_fichero = "fichero.txt"

class Master(object):
    _tell = ['readFile']
    _ask = []

    def readFile(self, host):
        os.chdir("./files")
        try:
            global num_slaves
            num_slaves = int(sys.argv[1])
            file = io.open(nombre_fichero, "r", encoding="utf-8")
            num_lines_file = int(commands.getoutput("wc -l "+nombre_fichero+" | cut -d ' ' -f 1"))
        except IndexError:
            print "===\nERROR. Pon un numero de slaves.\n==="
        except IOError:
            print "===\nERROR. Ha habido un problema al leer el fichero.\n==="
        finally:
            num_lines_map = num_lines_file/(num_slaves-1)
            if (num_lines_file % (num_slaves-1)) != 0:
                num_lines_map += 1
            slaves = {}
            for slave in range(0,num_slaves):                               #Creating slave's array
                if slave == num_slaves-1:
                    slaves[slave] = host.spawn('Reducer', 'reduce/Reducer')
                else:
                    slaves[slave] = host.spawn('Mapper_'+str(slave), 'mapper/Mapper')
            for act_slave in range(0,num_slaves-1):
                file_out = io.open("file_"+str(act_slave)+".txt", "w", encoding="utf-8")
                for act_line in range(0,num_lines_map):
                    line = file.readline()
                    if line != "":
                        file_out.write(line)
            file.close()
            file_out.close()


# Main
if __name__ == "__main__":
    set_context()
    host_master = create_host(url_host)
    master1 = host_master.spawn('master', 'master/Master')
    print("Listening server at port "+url_host[-5:-1])
    master1.readFile(host_master)
    serve_forever()
