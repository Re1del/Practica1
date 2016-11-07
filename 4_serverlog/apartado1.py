# coding=utf-8
import re
import sys

from pysparkling import Context

'''
Viktor Jacynycz García y Miguel del Andrés Herrero declaramos que esta solución es fruto
exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona
ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra actividad
que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
'''


def extract(x):
    y = pat.search(x)
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
    return ip, (1, num, data)


def add(x, y):
    return (x[0] + y[0], x[1] + y[1], x[2] + y[2])


sc = Context()

file1 = sys.argv[1]
lines = sc.textFile(file1)

# (.*?)\s-\s-.*?\s(\d+)\s(\d+|-) --> regexp
pat = re.compile('(.*?)\s-\s-[\w\W]+?\s(\d+)\s(\d+|-)')

rdd = (
    lines.filter(lambda x: pat.match(x))
        .map(lambda x: extract(x))
        .reduceByKey(lambda x, y: add(x, y))
)

vals = rdd.collect()

for item in vals:
    print item
