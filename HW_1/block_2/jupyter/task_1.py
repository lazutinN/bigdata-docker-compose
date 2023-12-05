from mrjob.job import MRJob
from mrjob.step import MRStep

class TopCharacters(MRJob):
    def mapper(self, _, line):
        characterName = line.split(" ")[1]
        yield characterName, 1
    
    def reducer(self, characterName, characterCount):
        yield None, (characterName, sum(characterCount))   

    def reducer_top(self, _, characterCount):
        top_character = sorted(characterCount, key=lambda x: x[1], reverse=True)[:20]
        for character, count in top_character:
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
    TopCharacters.run()