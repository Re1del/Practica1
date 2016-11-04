import sys

from pysparkling import Context

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
