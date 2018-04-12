# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever, sleep, Host
import io, os, sys, commands, urllib
global registry, remote_reduce, list_mappers


class Registry(object):
    _tell = ['bind']
    _ask = ['getMap']
    _ref = []

    def __init__(self):
        self.nslaves = 0

    def bind(self, name):
        print "\nServer registred ", name
        self.nslaves+=1

    def getMap(self):
        return self.nslaves

def splitFile(name_f, ip_files, num_mappers):
    try:
        os.chdir("./files")
        os.system("\nwget http://%s:8000/%s"%(ip_files,name_f))                                             #We download the file from server
        num_lines_map = int(commands.getoutput("wc -l "+name_f+" | cut -d ' ' -f 1"))/(num_mappers)         #split -l <num_lines> <name_f>
        os.system("split -l "+str(num_lines_map+1)+" "+name_f)
        os.system("rm "+name_f)
    except IOError:
        print "\n\tERROR. Ha habido un problema al leer el fichero."
        shutdown()

def create_hosts():
    global registry, remote_reduce, list_mappers
    list_mappers = {}
    host_master = create_host("http://%s:1500/" % ip_files)
    print "-------------MAP REDUCE----------------\nHosts:\nListening Master Server at port 1500"
    registry = host_master.spawn("regis", 'master/Registry')
    for i in range(0, num_mappers):
        host_mapper = create_host('http://%s:160%s/' % (ip_files, i))
        list_mappers[i] = host_mapper.spawn("Mapper", 'mapper/Mapper')
        print "Server Mapper %s at port 160%s" %(i, i)
    host_reducer = create_host('http://%s:1700/'%ip_files)
    remote_reduce = host_reducer.spawn('Reducer', 'reduce/Reducer')
    print "Server Reducer at port 1700\n"
    os.chdir("../examples")
    os.system("python -m SimpleHTTPServer &")
    os.chdir("../project")

def waitMappers():
    print "\n\tEsperando a mappers..."
    while num_mappers != registry.getMap():
        sleep(1)

# Main
if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 4:
            raise IndexError
        num_mappers = int(sys.argv[1])
        ip_files = str(sys.argv[2])
        program = str(sys.argv[3])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython master.py [numero_mappers] [ip_servidor_archivos*] [WC ó CW]\n\n\t* si ip_servidor_archivos es 'localhost' = 127.0.0.1\n"
    finally:
        if ip_files == "localhost":
            ip_files = "127.0.0.1"
        create_hosts()
        name_file = raw_input("\nNombre del fichero: ")
        repeticiones = raw_input("\nNúmero de repeticiones del archivo (1 = Lectura normal): ")
        waitMappers()
        if repeticiones != 1:
            os.system("python ../examples/script.py %s %s" % (name_file, repeticiones))
            print "python ../examples/script.py %s %s" % (name_file, repeticiones)
            name_file = "Extended_"+name_file
        remote_reduce.start(num_mappers)
        splitFile(name_file, ip_files, num_mappers)
        remote_reduce.startCrono()
        for i in range(0, num_mappers):
            list_mappers[i].map(remote_reduce, ip_files, program, i)
        serve_forever()
