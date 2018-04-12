# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown
import io, sys, urllib, collections, os
global num_mappers
# Mapper
class Mapper(object):
    _tell = ['start','map']             #Methods asincronous
    _ask = []
    _ref = ['map']              #Methods which we get a reference
    '''
    def cw(self, word_split):
        for word in word_split:
            self.words["Paraula"] = self.words.get("Paraula", 0) + 1
    '''
    def wc(self, word_split):
        for word in word_split:
            if word != "":
                self.words[word] = self.words.get(word, 0) + 1  # Get -> Getordefault

    def map(self, remote_reducer, ip_files, program, id_mapper):
        self.words = {}
        try:
            text = open(("x%s%s"%(chr((id_mapper/26)+97), chr((id_mapper%26)+97))), "r").read().lower()
        except IOError:
            print "\n\nERROR. No se puede abrir el archivo desde el mapper.\n"
        finally:
            text = text.replace('.', '').replace(',', '').replace(':', '').replace('\n',' ').replace('\t','').replace(';','').replace("!", "").replace("?", "").replace("-","")
            word_split = text.split()
            if program == 'WC':
                self.wc(word_split)
                remote_reducer.wc(self.words)
            elif program == 'CW':
                for word in word_split:
                    self.words["Paraula"] = self.words.get("Paraula", 0) + 1
                remote_reducer.cw(self.words)
            else:
                print "\n\tEl programa seleccionado no se encuentra disponible.\n"

if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 2:
            raise IndexError
        id_mapper = int(sys.argv[1])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython [nombre_archivo] [id_mapper] [ip_servidor_archivos*]\n\n\t* si ip_servidor_archivos es 'localhost' = 127.0.0.1\n"
        shutdown()
    finally:
        host = create_host('http://127.0.0.1:190%s/' % str(id_mapper))
        print "\n\tCargando...\n"
        # Reference to hosts
        remote_mapper = host.lookup_url("http://127.0.0.1:160%s/Mapper"%str(id_mapper), 'Mapper', 'mapper')
        remote_master = host.lookup_url("http://127.0.0.1:1500/regis", 'Registry', 'master')
        remote_master.bind("Mapper_%s"%str(id_mapper))
        print remote_mapper
