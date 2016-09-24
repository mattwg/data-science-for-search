from mrjob.job import MRJob
import StringIO
import csv

class MRInvertedIndex(MRJob):

    def mapper(self, _, value):
        # << IMPLEMENT MAPPER >> CODE HERE
        ## HIDE
        for row in csv.reader(StringIO.StringIO(value)):
            id = row[0]
            for term in row[1].lower().split():
                    yield term, id
                    
    def reducer(self, key, values):
        # << IMPLEMENT MAPPER >> CODE HERE
        ## HIDE
        for doc in values:
            yield key, doc

if __name__ == '__main__':
    MRInvertedIndex.run()