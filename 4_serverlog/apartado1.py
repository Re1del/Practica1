# coding=utf-8
import re
import sys
import pyspark

from pyspark import AccumulatorParam


'''
Viktor Jacynycz García y Miguel del Andrés Herrero declaramos que esta solución es fruto
exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona
ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra actividad
que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
'''


sc = pyspark.SparkContext(appName="myAppName")
class StrAccumulatorParam(AccumulatorParam):

    def zero(self, value):
        self.list = []

    def addInPlace(self, val1, val2):
        self.list.append(val2)
        return self.list



invalidLinesAccumulator = sc.accumulator("",StrAccumulatorParam())
emptyLinesAccumulator = sc.accumulator(0)


def extract(x):
    global invalidLinesAccumulator
    global emptyLinesAccumulator
    if pat.match(x):
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
    else:
        if x.strip():
            invalidLinesAccumulator += x
        else:
            emptyLinesAccumulator +=1
        return "zzzz----"+x,(0,0,-1)


def add(x, y):
    return (x[0] + y[0], x[1] + y[1], x[2] + y[2])




file1 = sys.argv[1]
lines = sc.textFile(file1)

# (.*?)\s-\s-.*?\s(\d+)\s(\d+|-) --> regexp
pat = re.compile('(.*?)\s-\s-[\w\W]+?\s(\d+)\s(\d+|-)')



rdd = (
    lines.map(lambda x: extract(x))
        .reduceByKey(lambda x, y: add(x, y))
        .sortByKey(True)
)

vals = rdd.collect()

for item in vals:
    print item

print invalidLinesAccumulator
print emptyLinesAccumulator