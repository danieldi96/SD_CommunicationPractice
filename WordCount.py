#!/usr/bin/env python
# -*- coding: utf-8 -*-

import string 

fitxer = open("ProvaWC.txt", 'r')
linia = fitxer.read()

wordcount={}
for word in string.split(linia):
	if word not in wordcount:
		wordcount[word]=1
	else:
		wordcount[word] += 1


#print sorted(wordcount)
print wordcount

fitxer.close()
