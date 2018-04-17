# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever, sleep, Host
import io, os, sys, commands, urllib
global registry, remote_reduce, list_mappers, host_master


class Registry(object):
    _tell = ['bind', 'results']
    _ask = ['getMap']
    _ref = []

    def __init__(self):
        """
        Constructor
        """
        self.nslaves = 0

    def bind(self, name):
        """
        Bind a name to an spawned reference of an actor.
        :param name: name to be recognized with.
        """
        self.nslaves+=1
        print "\nServer registred %s\n" % name

    def getMap(self):
        """
        Obtain the mappers.
        :return: the number of mappers spawned.
        """
        return self.nslaves

    def results(self,program, dicc, time):
        """
        Print results to terminal
        """
        if program == "CW":
            print "\n\n\tEl archivo tiene en total %s palabras." % dicc["Word"]
        else:
            for key in dicc.keys():
                print "%s : %s" % (key, dicc[key])
            print "\n\n\tLas palabras repetidas son: (<palabra, #veces>)"
        print "\n\n\tHa tardado %s segundos" % time

def splitFile(name_f, ip_files, num_mappers):
    """
    Split the file wich will be read later.
    :param name_f: name of the file.
    :param ip_files: ip of the Server files.
    :param num_mappers: number of mappers.
    """
    try:
        os.chdir("./files")
        os.system("wget http://%s:8000/%s"%(ip_files,name_f))                                             #We download the file from server
        num_lines_map = int(commands.getoutput("wc -l "+name_f+" | cut -d ' ' -f 1"))/(num_mappers)         #split -l <num_lines> <name_f>
        os.system("split -l "+str(num_lines_map+1)+" "+name_f)
        os.system("rm "+name_f)
    except IOError:
        print "\n\tERROR. Ha habido un problema al leer el fichero."
        shutdown()

def create_hosts():
    """
    Creation of master's Host and HTTP Server
    """
    global host_master, registry
    host_master = create_host("http://%s:1500/" % ip_sv)
    print "-------------MAP REDUCE----------------\nHosts:\nListening Master Server at port 1500"
    registry = host_master.spawn("regis", 'master/Registry')
    os.chdir("../examples")
    os.system("python -m SimpleHTTPServer &")
    os.chdir("../project")
    sleep(1)

def waitMappers():
    """
    Wait for all slaves to be done.
    """
    print "\n\tEsperando a mappers..."
    while num_mappers != registry.getMap()-1:
        sleep(1)

def lookups():
    """
    Getting all proxy's
    """
    global list_mappers, remote_reduce
    list_mappers = {}
    for i in range(0, num_mappers):
        list_mappers[i] = host_master.lookup_url("http://%s:160%s/Mapper" % (ip_files, str(i)), "Mapper", "mapper")
        print list_mappers[i]
    remote_reduce = host_master.lookup_url("http://%s:1700/Reducer" % ip_files, "Reducer", "reduce")
    print remote_reduce
    remote_reduce.start(num_mappers, registry)

# Main
if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 5:
            raise IndexError
        num_mappers = int(sys.argv[1])
        ip_sv = str(sys.argv[2])
        ip_files = str(sys.argv[3])
        program = str(sys.argv[4])
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
        lookups()
        if repeticiones != 1:
            os.chdir("../examples")
            os.system("python ../examples/script.py %s %s" % (name_file, repeticiones))
            name_file = "Extended_"+name_file
            os.chdir("../project")
        splitFile(name_file, ip_files, num_mappers)
        remote_reduce.startCrono()
        for i in range(0, num_mappers):
            list_mappers[i].map(remote_reduce, ip_files, program, i)
        serve_forever()
