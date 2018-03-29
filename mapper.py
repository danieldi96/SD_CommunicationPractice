from pyactor.context import set_context, create_host, Host, sleep, shutdown
global red
# Mapper

class Mapper(object):
    _tell = []
    _ask = ["map"]

    def map(self, num_line, line):
		words = {}
		line = line.lower()
		line = line.replace('.', '').replace(',', '').replace(':', '').replace('\n','').replace('\t','')
		word_split = line.split(" ")
		for word in word_split:
			words[word] = words.get(word, 0) + 1  # Get -> Getordefault
			print (words, 1)
		return words

if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1679')
    remote_master = host.lookup_url('http://127.0.0.1:1555/master1', 'Master', 'master')
    remote_reduce = host.lookup_url('direccion', 'Reducer', 'reduce')
