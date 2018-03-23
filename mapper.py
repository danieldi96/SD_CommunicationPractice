
import sys

#Mapper 
def map(num_line, line):
	words={}
    line = line.lower()
    line = line.replace('.', '').replace(',', '').replace(':', '')
	word_split=line.split(" ")
	for word in word_split:
		if word.endwith("\n"):
			counts[word]=counts.get(word,0) + 1		#Get -> Getordefault
		print (words, 1)
	return words
'''
def mapper(line):
    """ Map function definition. Splits the lines and generates key-value for
        each word.
    """
    counts = {}
    line = line.lower()
    line = line.replace('.', '').replace(',', '').replace(':', '')
    words = line.split()
    for word in words:
        counts[word] = counts.get(word, 0) + 1
    return counts
'''