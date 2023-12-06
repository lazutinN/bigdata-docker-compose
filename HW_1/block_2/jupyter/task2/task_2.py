
from mrjob.job import MRJob
from mrjob.step import MRStep


class LongestPhrase(MRJob):
    def mapper(self, _, line):
        parts = line.split(" ", 2)
        if len(parts) == 3:
            _, characterName, phrase = parts
            yield characterName, phrase

    def reducer(self, characterName, phrase):
        longestPhrase = max(phrase, key=len)
        yield None, (characterName, longestPhrase)

    def reducer_top(self, _, longestPhrase):
        sortedLongest = sorted(longestPhrase, key=lambda x: len(x[1]), reverse=True)
        for character, count in sortedLongest:
            yield character, count

    def steps(self):
        return [
            MRStep(
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                   reducer=self.reducer_top)
        ]
    
if __name__ == '__main__':
    LongestPhrase.run()
