from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, value):
        yield "chars", len(value)
        yield "words", len(value.split())
        yield "lines", 1
        

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()