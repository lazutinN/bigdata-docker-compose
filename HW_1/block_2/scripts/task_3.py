import nltk

from mrjob.job import MRJob
from mrjob.step import MRStep

class frequencyAnalysis(MRJob):
    """
    Маппер:
    1. Чистим от пунктуации и приводим к нижнему регистру
    2. Разбиваем на bigram'ы
    
    Редусер:
    1. Считаем частоты
    2. Сортируем по частоте в порядке убывания
    3. Выводим первые 20
    """

    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')
        self.stop_words = set(nltk.corpus.stopwords.words('english'))

    def mapper(self, _, line):
        parts = line.split(" ", 2)
        if len(parts) == 3:
            _, characterName, phrase = parts
            phrase = phrase.lower()
            phrase = nltk.word_tokenize(phrase)
            phrase = [w for w in phrase if w not in self.stop_words and w.isalpha()]
            for bigram in nltk.bigrams(phrase):
                yield bigram, 1

    def reducer(self, bigram, count):
        yield None, (bigram, sum(count))

    def reducer_top(self, _, bigramCount):
        sortedBigrams = sorted(bigramCount, key=lambda x: x[1], reverse=True)
        for bigram, count in sortedBigrams[:20]:
            yield bigram, count

    def steps(self):
        return [
            MRStep(
                   mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                   reducer=self.reducer_top)
        ]

if __name__ == '__main__':
    frequencyAnalysis.run()