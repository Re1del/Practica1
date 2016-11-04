import os
import re
import sys
from collections import Counter

from mrjob.job import MRJob


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
