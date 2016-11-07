import sys

from pysparkling import Context

'''
Viktor Jacynycz García y Miguel del Andrés Herrero declaramos que esta solución es fruto
exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona
ni hemos obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra actividad
que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
'''

if len(sys.argv) != 2:
    print "Falta el fichero!"
    exit(-1)

sc = Context()
lines = sc.textFile(sys.argv[1])

counts = (
    lines.filter(lambda x: float(x.split()[2]) < 2)
        .filter(lambda x: x.split()[4] != "--")
        .map(lambda x: (float(x.split()[2]), x.split()[0]))
        .sortByKey(False)
)

output = counts.take(5)
print '\n\n', output, '\n\n'
