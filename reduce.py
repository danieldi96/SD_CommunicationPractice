from pyactor.context import set_context, create_host, Host, sleep, shutdown
import collections
import sys
global dicc, num_slaves, dictionary_wc
dicc = {}
dictionary_wc = {}
class Reducer(object):
    _tell = ['shuffle','reduce', 'wc', 'cw']
    _ask = [] 		#sincrono
    _ref = []

    def __init__(self):
        global num_slaves
        try:
            num_slaves = int(sys.argv[1])
        except IndexError:
            print ""

    def shuffle(self, dic_map):
        global dicc
        for key in dic_map.keys():
            if key in dicc.keys():
                dicc[key] = dicc.get(key, 0) + dic_map[key]      #si ya esta en el diccionario le sumamos 1
            else:
                dicc[key] = dic_map[key]           #si no esta, anadimos la clave al nuevo diccionario con las ocurrencias de esta en la linea
        return dicc

    def wc(self, dic_map, id_map):
        global dictionary_wc
        dictionary_wc = self.shuffle(dic_map)
        if int(id_map) == (num_slaves-2):
            file_out = open("palabras_repetidas.txt","w")
            sleep(3)
            print "\n\tLas palabras repetidas son: (<palabra, #veces>)"
            file_out.write("\n\tLas palabras repetidas son: (<palabra, #veces>)\n")
            for key in dictionary_wc.keys():
                line_dic = key+" : "+str(dictionary_wc[key])
                print line_dic
                file_out.write(line_dic)
            print "\n\n\tPuede encontrar las palabras repetidas en: ./files/palabras_repetidas.txt"


    def cw(self, dic_map, id_map):
        dictionary = self.shuffle(dic_map)
        res = 0
        for key in dictionary.keys():
            res = res + dictionary[key]
        if int(id_map) == (num_slaves-2):
            sleep(59)
            print "\t El archivo tiene en total "+str(res)+" palabras."
