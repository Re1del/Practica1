# coding=utf-8
import os
import re
import sys
from collections import Counter

from mrjob.job import MRJob

'''
Viktor Jacynycz García y Miguel del Andrés Herrero declaramos que esta solución es fruto
exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona
ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra actividad
que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
'''


class MRApartado3(MRJob):
    def mapper(self, key, line):
        filename = os.environ['mapreduce_map_input_file']
        wordList = re.sub("[^\w]", " ", line).split()
        for word in wordList:
            yield word.lower(), filename

    def reducer(self, key, value):
        counts = Counter(value).items()
        filter = False
        for v in counts:
            if v[1] >= 20:
                filter = True
        if filter:
            yield key, counts


if __name__ == '__main__':
    print sys.argv
    MRApartado3.run()
