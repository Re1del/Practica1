# coding=utf-8
import os
import re
import sys

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
        counts = dict()
        for word in wordList:
            counts[filename] = 1
            yield word.lower(), counts

    def combiner(self, key, values):
        counts = dict()
        for dictionaries in values:
            for item in dictionaries.items():
                if counts.has_key(item[0]):
                    counts[item[0]] = counts[item[0]] + item[1]
                else:
                    counts[item[0]] = item[1]
        yield key, counts

    def reducer(self, key, values):
        counts = dict()
        filter = False
        for dictionaries in values:
            for item in dictionaries.items():
                if counts.has_key(item[0]):
                    counts[item[0]] = counts[item[0]] + item[1]
                    if counts[item[0]] >= 20:
                        filter = True
                else:
                    counts[item[0]] = item[1]
                    if item[1] >= 20:
                        filter = True
        if filter:
            yield key, counts


if __name__ == '__main__':
    print sys.argv
    MRApartado3.run()
