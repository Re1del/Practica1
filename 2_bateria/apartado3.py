# coding=utf-8
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

class MRApartado2_3(MRJob):
    def mapper(self, key, line):
        linesplit = line.split(',')
        p = re.compile('(\d{4}\/\d{2})')
        m = p.match(linesplit[0])
        if m:
            my = m.group()
            # print my + ',' + linesplit[8]
            yield my, linesplit[8]

    def combiner(self, key, value):
        len = 0
        min = 0
        total = 0
        max = 0
        for i in value:
            curr_num = float(i)

            if len == 0:
                min = curr_num
            else:
                if curr_num <= min:
                    min = curr_num

            total = total + curr_num

            if curr_num >= max:
                max = curr_num

            len = len + 1
        yield key, (max, min, total, len)

    def reducer(self, key, value):
        len = 0
        min = 0
        total = 0
        max = 0
        for i in value:
            if len == 0:
                max = i[0]
                min = i[1]
                total = i[2]
                len = i[3]
            else:
                if i[0] >= max:
                    max = i[0]
                if i[1] <= min:
                    min = i[0]
                total = total + i[2]
                len = len + i[3]
        avg = total / len
        yield key, (max, avg, min)


if __name__ == '__main__':
    print sys.argv
    if len(sys.argv) != 3:
        print "Faltan los ficheros!"
        exit(-1)
    MRApartado2_3.run()
