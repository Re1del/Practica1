import re
import sys

from pysparkling import Context

if len(sys.argv) != 3:
    print "Falta el fichero!"
    exit(-1)

sc = Context()
files = sys.argv[1] + "," + sys.argv[2]
lines = sc.textFile(files)
p = re.compile('(\d{4}\/\d{2})')


def find_values(set):
    len = 0
    min = 0
    avg = 0
    max = 0
    value = set[1]
    key = set[0]
    for i in value:
        curr_num = float(i)

        if len == 0:
            min = curr_num
        else:
            if curr_num <= min:
                min = curr_num

        avg = avg + curr_num

        if curr_num >= max:
            max = curr_num

        len = len + 1
    avg = avg / len
    # print  key, (min,avg,max)
    return key, (min, avg, max)


rdd_core = (
    lines.filter(lambda x: p.match(x))
        .map(lambda x: (p.search(x).group(), float(x.split(',')[8])))
        .groupByKey()
        .map(lambda x: find_values(x))
)

rdd_max = rdd_core.collect()
print rdd_max
