from mrjob.job import MRJob
import StringIO
import csv

class MRTermFrequencyCount(MRJob):

    def mapper(self, _, value):
        # << IMPLEMENT MAPPER >> CODE HERE
        ## HIDE
        for row in csv.reader(StringIO.StringIO(value)):
            for term in row[1].lower().split():
                    yield term, 1

    def reducer(self, key, values):
        # << IMPLEMENT REDUCER >> CODE HERE
        ## HINT
        yield key, sum(values)

if __name__ == '__main__':
    MRTermFrequencyCount.run()