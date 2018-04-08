# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown
import io
import sys
import urllib
global ip_files, num_slaves, program
ip_files = "192.168.1.100:8000"

# Mapper
class Mapper(object):
    _tell = ['map']             #Methods asincronous
    _ask = []
    _ref = ['map']              #Methods which we get a reference

    def __init__(self):
        global num_slaves, program
        try:
            num_slaves = int(sys.argv[1])
        except IndexError:
            print "===\nERROR. Pon un numero de slaves\n"
        try:
            program = str(sys.argv[2])
        except IndexError:
            print  "===\nERROR. Pon un tipo de programa.\n'WC' = WordCount \n'CW' = CountingWords\n==="

    def map(self, remote_reducer):
        words = {}
        url = "http://"+ip_files+"/file_"+str(self.id)[-1:]+".txt"
        file = urllib.urlopen(url)
        text = file.read().decode("latin-1")
        text = text.lower()
        print "\n"+str(self.id)+"\n\nText:\n"+text #).encode("utf-8")
        text = text.replace('.', '').replace(',', '').replace(':', '').replace('\n',' ').replace('\t','').replace(';','')
        #line_split = text.split("\n")
        word_split = text.split(" ")
        #print word_split
        for word in word_split:
            if word != "":
                word = word.encode("latin-1")
                if word.endswith('-') or word.startswith('-'):  #eliminamos los guiones de las conversaciones
                    word.replace('-','')
                words[word] = words.get(word, 0) + 1  # Get -> Getordefault
        if program == 'WC':
            remote_reducer.wc(words, self.id[-1:])
        elif program == 'CW':
            remote_reducer.cw(words, self.id[-1:])


if __name__ == "__main__":
    global num_slaves
    set_context()
    host = create_host('http://127.0.0.1:2805')

    # We get the reference of master (server) and we create inmediately
    # a instance of Mapper class in it.

    # Reference to hosts
    remote_reduce = host.lookup_url("http://127.0.0.1:1700/Reducer", 'Reducer', 'reduce')
    try:
        num_slaves = int(sys.argv[1])
    except IndexError:
        print "===\nERROR. Pon un numero de slaves\n"
    for i in range(0,num_slaves-1):
        remote_mapper = host.lookup_url("http://127.0.0.1:1600/Mapper_"+str(i), 'Mapper', 'mapper')
        print remote_mapper
        remote_mapper.map(remote_reduce)
    sleep(3)
    shutdown()
