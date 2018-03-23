from pyactor.context import set_context, create_host, shutdown
import mapper as map

def readFile():
	i = 0
	with open("fichero.txt", "r") as file_open:
 		map.map(i,file_open.readLine()) #map recibe el numero de lunea y la linea entera
		i+=1
	file_open.close()

class Echo(object):
    _tell = ['echo']
    _ask = ['get_msgs']

    def __init__(self):
        self.msgs = []

    def echo(self, msg):
        print msg
        self.msgs.append(msg)

    def get_msgs(self):
        return self.msgs


if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1277/')

    e1 = host.spawn('echo1', Echo)
    serve_forever()