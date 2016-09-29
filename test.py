

from pyspark import SparkContext


sc = SparkContext('local', 'osp')

test = sc.textFile('./test.txt')

word_count = (
    test
    .map(lambda line: len(line.split()))
    .reduce(lambda a, b: a+b)
)

print(word_count)
