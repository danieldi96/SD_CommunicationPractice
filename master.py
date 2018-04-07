# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever
import io
import os
import sys
import commands
import urllib
global dictionary, url_host, nombre_fichero, num_slaves

# Global variables
url_host = "http://127.0.0.1:1500/"
ip_files = "192.168.1.100:8000"
nombre_fichero = "fichero.txt"

class Master(object):
    _tell = ['readFile']
    _ask = []

    def readFile(self, host_map, host_red):
        url = "http://"+ip_files+"/"+nombre_fichero
        urllib.urlretrieve(url, nombre_fichero)                 #We download the file from server
        try:
            global num_slaves
            num_slaves = int(sys.argv[1])
            file = io.open(nombre_fichero, "r", encoding="latin-1")         #We read in Latin to read all chars of Spanish
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
                    slaves[slave] = host_red.spawn('Reducer', 'reduce/Reducer')
                else:
                    slaves[slave] = host_map.spawn('Mapper_'+str(slave), 'mapper/Mapper')
            os.chdir("./files")
            for act_slave in range(0,num_slaves-1):
                file_out = io.open("file_"+str(act_slave)+".txt", "w", encoding="latin-1")      #We read in Latin to read all chars of Spanish
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
    #remote_reduce = host_reducer.spawn('Reducer', 'reduce/Reducer')
    print("Listening server at port "+url_host[-5:-1])
    host_mapper = create_host('http://127.0.0.1:1600/')
    print "Server Mapper at port 1600"
    host_reducer = create_host('http://127.0.0.1:1700/')
    print "Server Reducer at port 1700"
    master1.readFile(host_mapper, host_reducer)
    serve_forever()
