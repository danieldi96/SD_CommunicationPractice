# -*- coding: utf-8 -*-

from pyactor.context import set_context, create_host, shutdown, serve_forever, sleep, Host
import io, os, sys, commands, urllib
global registry, list_slaves, host_master


class Registry(object):
    _tell = ['bind', 'results']
    _ask = ['getMap', 'getActors']
    _ref = ['bind', 'getActors']

    def __init__(self):
        """
        Constructor
        """
        self.actors = {}
        self.nslaves = 0

    def bind(self, name, actor):
        """
        Bind a name to an spawned reference of an actor.
        :param name: name to be recognized with.
        """
        self.actors[self.nslaves] = actor
        self.nslaves+=1
        print "\nServer registred %s\n" % name

    def getMap(self):
        """
        Obtain the mappers.
        :return: the number of mappers spawned.
        """
        return self.nslaves

    def getActors(self):
        return self.actors

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

def splitFile(name_f, ip_slaves, num_mappers):
    """
    Split the file wich will be read later.
    :param name_f: name of the file.
    :param ip_slaves: ip of the Server files.
    :param num_mappers: number of mappers.
    """
    try:
        os.system("wget http://%s:8000/%s"%(ip_slaves,name_f))                                             #We download the file from server
        num_lines_map = int(commands.getoutput("wc -l "+name_f+" | cut -d ' ' -f 1"))/(num_mappers)         #split -l <num_lines> <name_f>
        os.system("split -l "+str(num_lines_map+1)+" "+name_f)
        os.system("rm "+name_f)
        os.system("mv ./x* ../../examples")
    except IOError:
        print "\n\tERROR. Ha habido un problema al leer el fichero."
        shutdown()

def create_hosts():
    """
    Creation of master's Host and HTTP Server
    """
    global host_master, registry
    print "http://%s:1500/" % ip
    host_master = create_host("http://%s:1500/" % ip)
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
    global list_slaves
    list_slaves = registry.getActors()
    '''
    for i in range(0, num_mappers):
        list_slaves[i] = host_master.lookup_url("http://%s:160%s/Mapper" % (ip_slaves, str(i)), "Mapper", "mapper")
        print list_slaves[i]
    list_slaves[num_mappers] = host_master.lookup_url("http://%s:1700/Reducer" % ip_slaves, "Reducer", "reduce")
    print list_slaves[num_mappers]
    '''
    list_slaves[num_mappers].start(num_mappers, registry)

# Main
if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 4:
            raise IndexError
        num_mappers = int(sys.argv[1])
        ip = str(sys.argv[2])
        program = str(sys.argv[3])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython master.py [numero_mappers] [ip_master] [WC ó CW]\n\n\t* si las ip es 'localhost' = 127.0.0.1\n"
    finally:
        if ip == "localhost":
            ip = "127.0.0.1"
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
        splitFile(name_file, ip, num_mappers)
        list_slaves[num_mappers].startCrono()
        for i in range(0, num_mappers):
            list_slaves[i].map(list_slaves[num_mappers], ip, program, i)
        serve_forever()
