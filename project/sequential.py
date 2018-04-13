# -*- coding: utf-8 -*-
import time
import sys, os
global timeTotal
#Input:
#   num:     0 : Start crono
#            1 : Stop crono
def crono(num):
    global timeTotal
    if num == 0:
        timeTotal = time.time()
    elif num == 1:
        timeend = time.time()
        timeTotal = timeend - timeTotal
def wc(word_split):
    for word in word_split:
        if word != "":
            if word.endswith("-") or word.startswith("-"):
                word.replace("-", "")
            words[word] = words.get(word, 0) + 1

def cw(word_split):
    key = "Word"
    for word in word_split:
        words[key] = words.get(key, 0) + 1

if __name__ == "__main__":
    program = str(sys.argv[1])
    name_file = raw_input("\nIndica el nombre del archivo: ")
    repeticiones = raw_input("\nNúmero de repeticiones del archivo (1 = Lectura normal): ")
    os.chdir("../examples")
    if repeticiones != 1:
        os.system("python ../examples/script.py %s %s" % (name_file, repeticiones))
        name_file = "Extended_"+name_file
    text = open(name_file, "r").read()
    timeTotal = 0
    words = {}
    crono(0)
    for char in ".,:;!?()[]'\t'":
        text = text.replace(char, "")
    text = text.replace('\n',' ')
    word_split = text.split()
    if program == "WC":
        wc(word_split)
        for key in words.keys():
            print "%s : %s" % (key, words[key])
        print "\n\n\tLas palabras repetidas son: (<palabra, #veces>)"
    elif program == "CW":
        cw(word_split)
        print "\n\n\t El archivo tiene en total %s palabras." % words["Word"]
    else :
        print "\n\tEl programa seleccionado no se encuentra disponible.\n"
    crono(1)
    print "\n\n\tHa tardado %s segundos." % timeTotal
