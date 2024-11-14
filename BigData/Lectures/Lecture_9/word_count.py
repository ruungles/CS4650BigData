
from mrjob.job import MRJob

class WordFrequency(MRJob):

    def mapper(self, _, line):
        thelist = line.split()
        for x in thelist:
            yield x, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    WordFrequency.run()
