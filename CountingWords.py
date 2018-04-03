#!/usr/bin/env python
# -*- coding: utf-8 -*-

import string

fitxer = open("ProvaCW.txt", 'r')
linia = fitxer.read()

paraula = string.split(linia)
num = len(paraula)

print num
fitxer.close()
