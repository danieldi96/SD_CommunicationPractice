import sys

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
