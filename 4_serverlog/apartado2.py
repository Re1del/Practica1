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
        pat = re.compile('(.*?)\s-\s-[\w\W]+?\s(\d+)\s(\d+|-)')
        m = pat.match(line)
        if m:
            y = pat.search(line)
            ip = y.group(1)
            num = int(y.group(2))
            if num >= 400 | num < 600:
                num = 1
            else:
                num = 0

            data = y.group(3)
            if data == "-":
                data = 0
            else:
                data = int(data)
            yield ip, (1, num, data)

    def combiner(self, key, value):
        numpet = 0
        numerr = 0
        data = 0
        for item in value:
            numpet = numpet + item[0]
            numerr = numerr + item[1]
            data = data + item[2]
        yield key, (numpet, numerr, data)

    def reducer(self, key, value):
        numpet = 0
        numerr = 0
        data = 0
        for item in value:
            numpet = numpet + item[0]
            numerr = numerr + item[1]
            data = data + item[2]
        yield key, (numpet, numerr, data)


if __name__ == '__main__':
    print sys.argv
    MRApartado2_3.run()
