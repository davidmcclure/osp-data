

from pyspark import SparkContext


sc = SparkContext('local', 'osp')

test = sc.textFile('./test.txt')


res = (
    test
    .flatMap(lambda line: line.split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a+b)
)

print(res.collect())
