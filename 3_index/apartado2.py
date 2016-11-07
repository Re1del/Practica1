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

sc = Context()

file1 = sys.argv[1]
lines = sc.textFile(file1)

rdd_part_1= (
    lines.flatMap(lambda x: re.sub("[^\w]", " ", x).split())
        .map(lambda x: (x.lower(), 1))
        .reduceByKey(lambda x, y : x + y)
        .filter(lambda x : x[1] >= 20)
        .map(lambda x : (x[0] , (x[1],file1)))
)

file2 = sys.argv[2]
lines = sc.textFile(file2)

rdd_part_2= (
    lines.flatMap(lambda x: re.sub("[^\w]", " ", x).split())
        .map(lambda x: (x.lower(), 1))
        .reduceByKey(lambda x, y : x + y)
        .filter(lambda x : x[1] >= 20)
        .map(lambda x : (x[0] , (x[1],file2)))
)


file3 = sys.argv[3]
lines = sc.textFile(file3)

rdd_part_3= (
    lines.flatMap(lambda x: re.sub("[^\w]", " ", x).split())
        .map(lambda x: (x.lower(), 1))
        .reduceByKey(lambda x, y : x + y)
        .filter(lambda x : x[1] >= 20)
        .map(lambda x : (x[0] , (x[1],file3)))
)


rdd_max = sc.union([rdd_part_1,rdd_part_2,rdd_part_3]).groupByKey().sortByKey()


vals = rdd_max.collect()

for item in  vals:
    print item
