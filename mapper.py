# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown
import io
import sys
import urllib
global ip_files, num_slaves
ip_files = "192.168.1.100:8000"

# Mapper
class Mapper(object):
    _tell = ['map']             #Methods asincronous
    _ask = []
    _ref = ['map']              #Methods which we get a reference

    def map(self, spawn_reducer):
        words = {}
        url = "http://"+ip_files+"/file_"+str(self.id)[-1:]+".txt"
        file = urllib.urlopen(url)
        text = file.read().decode("latin-1")
        text = text.lower()
        print "\n\nMapper "+str(self.id)+"\n"
        print text+"\n" #).encode("utf-8")
        text = text.replace('.', '').replace(',', '').replace(':', '').replace('\n',' ').replace('\t','').replace(';','')
        #line_split = text.split("\n")
        word_split = text.split(" ")
        print word_split
        for word in word_split:
            word = word.encode("latin-1")
            if word.endswith('-') or word.startswith('-'):  #eliminamos los guiones de las conversaciones
                word.replace('-','')
            words[word] = words.get(word, 0) + 1  # Get -> Getordefault
            print (words, 1)
        print "\n\n\n================="
        print words
        #spawn_reducer.shuffle(words)
        #return words

if __name__ == "__main__":
    set_context()
    global num_slaves
    num_slaves = int(sys.argv[1])
    host = create_host('http://127.0.0.1:2805')

    # We get the reference of master (server) and we create inmediately
    # a instance of Mapper class in it.

    # Reference to hosts
    remote_reduce = host.lookup_url("http://127.0.0.1:1700/", Host)

    # Spawns
    #spawn_reducer = remote_reduce.spawn('Reducer', 'reduce/Reducer')
    for i in range(0,num_slaves-1):
        remote_mapper = host.lookup_url("http://127.0.0.1:1600/Mapper_"+str(i), 'Mapper', 'mapper')
        #spawn_mapper = remote_mapper.spawn('Mapper_'+str(i), 'mapper/Mapper')
        print remote_mapper
        remote_mapper.map(remote_reduce)
    '''
    try:
        print spawn_reducer.wait_a_lot(timeout=1)
    except TimeoutError, e:
        print e
    '''
    sleep(3)
    shutdown()
