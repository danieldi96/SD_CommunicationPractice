from pyactor.context import set_context, create_host, Host, sleep, shutdown
import collections
import sys
global dicc, num_slaves
dicc = {}

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
        for key in dic_map:
            if key in dicc.keys():
                dicc[key] = dicc.get(key, 0) + 1      #si ya esta en el diccionario le sumamos 1
            else:
                dicc[key] = 1           #si no esta, anadimos la clave al nuevo diccionario con las ocurrencias de esta en la linea
        return dicc
        '''
        result1 = ""
        print "aqui"
        for key in dicc.keys():
            result1 = result1+"\n"+ reduce(key, [dicc.get(key, 0),0]) #se lo pasamos con un 0 para que sea una lista [valor,0]
        print "\n\n\nresult 1\n"
        print result1
    def reduce(self, clave, lista_valores):
        suma = 0
        for x in lista_valores:
            suma = suma + x			#suma cada elemento de la lista
        return clave+":"+str(suma)	#devuelve la clave concatenada con la suma de los valores (veces que aparece)
        '''
    def wc(self, dic_map, id_map):
        dictionary = self.shuffle(dic_map)
        if int(id_map) == (num_slaves-2):
            print "\n\tLas palabras repetidas son: (<palabra, #veces>)"
            for key in dictionary.keys():
                print key+" : "+str(dictionary[key])

    def cw(self, dic_map, id_map):
        dictionary = self.shuffle(dic_map)
        res = 0
        for key in dictionary.keys():
            res = res + dictionary[key]
        if int(id_map) == (num_slaves-2):
            sleep(3)
            print "\t El archivo tiene en total "+str(res)+" palabras."
