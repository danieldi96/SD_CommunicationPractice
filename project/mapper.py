# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown, serve_forever
import io, sys, urllib, collections, os
# Mapper
class Mapper(object):
    _tell = ['start','map']             #Methods asincronous
    _ask = []
    _ref = ['map']              #Methods which we get a reference

    def wc(self, word_split):
        for word in word_split:
            if word != "":
                if word.endswith("-") or word.startswith("-"):
                    word.replace("-", "")
                self.words[word] = self.words.get(word, 0) + 1  # Get -> Getordefault
        return self.words

    def map(self, remote_reducer, ip_files, program, id_mapper):
        self.words = {}
        try:
            self.text = open(("x%s%s"%(chr((id_mapper/26)+97), chr((id_mapper%26)+97))), "r").read().lower()
        except IOError:
            print "\n\nERROR. No se puede abrir el archivo desde el mapper.\n"
        finally:
            for char in ".,:;!?()[]'\t'":
                self.text = self.text.replace(char, "")
            self.text = self.text.replace('\n',' ')
            word_split = self.text.split()
            if program == 'WC':
                remote_reducer.wc(self.wc(word_split))
            elif program == 'CW':
                key = "Word"
                for word in word_split:
                    self.words[key] = self.words.get(key, 0) + 1
                remote_reducer.cw(self.words)
            else:
                print "\n\tEl programa seleccionado no se encuentra disponible.\n"

if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 3:
            raise IndexError
        id_mapper = int(sys.argv[1])
        ip = str(sys.argv[2])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython [nombre_archivo] [id_mapper] [ip_servidor_archivos*]\n\n\t* si ip_servidor_archivos es 'localhost' = 127.0.0.1\n"
        shutdown()
    finally:
        if ip == "localhost":
            ip = "127.0.0.1"
        host = create_host('http://127.0.0.1:200%s/' % str(id_mapper))
        print "\n\tCargando...\n"
        remote_mapper = host.lookup_url("http://%s:160%s/Mapper"%(ip, id_mapper), 'Mapper', 'mapper')
        remote_master = host.lookup_url("http://%s:1500/regis"%ip, 'Registry', 'master')
        remote_master.bind("Mapper_%s"%str(id_mapper))
        print remote_mapper
    serve_forever()
