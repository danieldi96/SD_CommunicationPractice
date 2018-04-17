# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, Host, sleep, shutdown, serve_forever
from collections import Counter
import io, sys, urllib, os
# Mapper
class Mapper(object):
    _tell = ['map']
    _ask = []
    _ref = ['map']

    def map(self, remote_reducer, ip_files, program, id_mapper):
        """
        Open the file, read it and move to lowercase. Replace the characters that can produce
        an error to a blank space. Then will execute the part of WordCount or CountingWords.
        On the WordCount part will count the times a word is repeated.
        On the CountingWord part will add all the word there is on the file.
        :param remote_reducer: reducer's proxy
        :param ip_files: ip of the Server files
        :param program: type of program CW || WC
        :param id_mapper: number of the mapper
        :return:
        """
        self.words = Counter()
        try:
            #os.chdir("./files")
            urllib.urlretrieve("http://%s:8000/x%s%s" % (ip_files, chr((id_mapper/26)+97), chr((id_mapper%26)+97)))
            #self.text = open(("x%s%s"%(chr((id_mapper/26)+97), chr((id_mapper%26)+97))), "r").read().lower()
        except IOError:
            print "\n\nERROR. No se puede abrir el archivo desde el mapper.\n"
        finally:
            for char in ".,:;!?()[]\"\''\t'":
                self.text = self.text.replace(char, "")
            self.text = self.text.replace('\n',' ')
            word_split = self.text.split()
            if program == 'WC':
                self.words.update(word_split)
                remote_reducer.wc(self.words)
            elif program == 'CW':
                self.words.update(Word = len(word_split))
                remote_reducer.cw(self.words)
            else:
                print "\n\tEl programa seleccionado no se encuentra disponible.\n"

if __name__ == "__main__":
    set_context()
    try:
        if len(sys.argv) != 4:
            raise IndexError
        id_mapper = int(sys.argv[1])
        ip = str(sys.argv[2])
        ip_sv = str(sys.argv[3])
    except IndexError:
        print   "\n----------------\nERROR. Los argumentos no son válidos.\n----------------\nArgumentos:"
        print   "\n\tpython master.py [id_mapper] [ip_mapper] [ip_master]\n\n\t* si las ip's son 'localhost' = 127.0.0.1\n"
        shutdown()
    finally:
        if ip == "localhost":
            ip = "127.0.0.1"
        if ip_sv == "localhost":
            ip_sv = "127.0.0.1"
        host = create_host("http://%s:160%s/" % (ip,id_mapper))
        map = host.spawn("Mapper", "mapper/Mapper")
        print map
        print "\n\tCargando...\n"
        remote_master = host.lookup_url("http://%s:1500/regis"%ip_sv, 'Registry', 'master')
        remote_master.bind("Mapper_%s"%str(id_mapper), map)
    serve_forever()
