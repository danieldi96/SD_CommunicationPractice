# -*- coding=UTF-8 -*-
'''
Script to repeat a file 'n' times.
@author: Daniel Díaz Fernández
'''

import sys

if __name__ == "__main__" :
    try:
        file_name = str(sys.argv[1])
        iterations = int(sys.argv[2])
    except IndexError:
        print "\n\n\tSyntax: python script.py <name_file_to_repeat> <num_repetitions>\n"
    finally:
        text = open(file_name, "r").read()
        file_out = open("Extended_"+file_name, "w")
        for i in range(0, iterations):
            file_out.write(text)
