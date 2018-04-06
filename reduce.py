from pyactor.context import set_context, create_host, Host, sleep, shutdown
import collections

class Reducer(object):
    _tell = []
    _ask = ['reduce'] 		#sincrono

    def shuffle(self, linea):
        global dicc
        for key in linea.keys():
            if key in dicc.keys():
                dicc[key] = dicc.get(key, 0) + linea.get(key, 0)      #si ya esta en el diccionario le sumamos 1
            else:
                dicc[key] = linea.get(key, 0)           #si no esta, anadimos la clave al nuevo diccionario con las ocurrencias de esta en la linea
        result1 = ""
        for key in dicc.keys():
            result1 = result1+"\n"+ reduce(key, [dicc.get(key, 0),0]) #se lo pasamos con un 0 para que sea una lista [valor,0]
        return result1

    def reduce(self, clave, lista_valores):
        suma = 0
        for x in lista_valores:
            suma = suma + x			#suma cada elemento de la lista
        return clave+":"+str(suma)	#devuelve la clave concatenada con la suma de los valores (veces que aparece)

if __name__ == "__main__":
    set_context()
    #host = create_host('http://127.0.0.1:1679')

    # We get the reference of master (server) and we create inmediately
    # a instance of Mapper class in it.
    # Reference to master (Server)
    remote_master = host.lookup_url('http://127.0.0.1:1576/', Host)
    # Spawn of Reducer class in master (Server)
    server = remote_master.spawn('Reduce1','reduce/Reducer')
    try:
        print e2.wait_a_lot(timeout=1)
    except TimeoutError, e:
        print e

    sleep(3)
    shutdown()
