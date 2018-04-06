# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown

# Mapper
class Mapper(object):
    _tell = []
    _ask = ["map"]

    def map(self, num_line, line):
        words = {}
        line = line.lower()
        line = line.replace('.', '').replace(',', '').replace(':', '').replace('\n','').replace('\t','').replace(';','')
        word_split = line.split(" ")
        print "Mapper "+str(self.id)
        for word in word_split:
            if word.endswith('-') or word.startswith('-'):  #eliminamos los guiones de las conversaciones
                word.replace('-','')
            words[word] = words.get(word, 0) + 1  # Get -> Getordefault
            print (words, 1)
        spawn_reducer.shuffle(words)
        return words

if __name__ == "__main__":
    global server, remote_master
    set_context()
    host_mapper = create_host('http://127.0.0.1:1680')
    host_reducer = create_host('http://127.0.0.1:1685')

    # We get the reference of master (server) and we create inmediately
    # a instance of Mapper class in it.

    # Reference to master and reducer
    remote_master = host.lookup_url("http://127.0.0.1:1576/", Host)
    #remote_reduce = host.lookup_url("http://127.0.0.1:1591/Reducer", 'Reducer', 'reduce')

    # Spawn of Mapper and Reducer class
    spawn_master = remote_master.spawn('mapper_'+self.id,'mapper/Mapper')
    spawn_reducer = host_reduce.spawn('reducer', 'reduce/Reducer')
    try:
        print spawn_reducer.wait_a_lot(timeout=1)
    except TimeoutError, e:
        print e
    sleep(3)
    shutdown()
