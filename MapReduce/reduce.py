from pyactor.context import set_context, create_host, Host, sleep, shutdown
import collections
import sys
global dicc, num_slaves, dictionary_wc, finished
dicc = {}
dictionary_wc = {}

class Reducer(object):
    _tell = ['shuffle', 'wc', 'cw', 'finished']
    _ask = [] 		#sincrono
    _ref = ['finished']

    def __init__(self):
        global num_slaves, finished
        try:
            num_slaves = int(sys.argv[1])
        except IndexError:
            print ""
        finally:
            finished = (num_slaves-1)

    def shuffle(self, dic_map):
        global dicc
        for key in dic_map.keys():
            if key in dicc.keys():
                dicc[key] = dicc.get(key, 0) + dic_map[key]      #si ya esta en el diccionario le sumamos 1
            else:
                dicc[key] = dic_map[key]           #si no esta, anadimos la clave al nuevo diccionario con las ocurrencias de esta en la linea
        return dicc

    def wc(self, dic_map, id_map):
        global dictionary_wc, finished
        dictionary_wc = self.shuffle(dic_map)
        finished -= 1
        if finished == 0:
            file_out = open("palabras_repetidas.txt","w")
            print "\n\tLas palabras repetidas son: (<palabra, #veces>)"
            file_out.write("\n\tLas palabras repetidas son: (<palabra, #veces>)\n")
            for key in dictionary_wc.keys():
                line_dic = key+" : "+str(dictionary_wc[key])
                print line_dic
                file_out.write(line_dic)
            print "\n\n\tPuede encontrar las palabras repetidas en: ./files/palabras_repetidas.txt"


    def cw(self, dic_map, id_map):
        global finished
        dictionary = self.shuffle(dic_map)
        res = 0
        for key in dictionary.keys():
            res = res + dictionary[key]
        finished -= 1
        if finished == 0:
            print "\n\n\t El archivo tiene en total "+str(res)+" palabras."

    def finished(self, host):
        print "\n\tEsperando...\n"
        if finished == 0:
            shutdown(host)
        return False
