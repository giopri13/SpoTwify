from __future__ import print_function

import os
import json
import sys
import pyspark
from pyspark import SparkContext
import pyspark.sql.types as tp
from pyspark.sql.functions import from_json, struct, to_json
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from elasticsearch import Elasticsearch
from datetime import datetime

print(os.path.realpath(__file__))
import sys
#reload(sys)

# Spark
sc = SparkContext(appName = "PythonSparkSA")
spark = SparkSession(sc)
sc.setLogLevel("WARN")

# Kafka
kafkaServer = "kafkaServer:9092"
brokers = "10.0.100.23:9092"
topic = "tweets"

# ElasticSearch
elastic_host = "10.0.100.51"
elastic_index = "twitter"
elastic_document = "_doc"

# Training Set
def training():
    trainingSchema = tp.StructType([
        tp.StructField(name = 'ItemID', dataType = tp.IntegerType(), nullable = True),
        tp.StructField(name = 'Sentiment', dataType = tp.IntegerType(), nullable = True),
        tp.StructField(name = 'SentimentSource', dataType = tp.StringType(), nullable = True),
        tp.StructField(name = 'SentimentText', dataType = tp.StringType(), nullable = True)
    ])

    training_set = spark.read.csv('../tap/spark/dataset/Sentiment Analysis Dataset.csv',
                                schema = trainingSchema,
                                header = True,
                                sep = ',').limit(150000)
    training_set

    # Pipeline
    stage_1 = RegexTokenizer(inputCol = 'SentimentText', outputCol = 'tokens', pattern = '\\W')
    stage_2 = StopWordsRemover(inputCol = 'tokens', outputCol = 'filtered_words')
    stage_3 = Word2Vec(inputCol = 'filtered_words', outputCol = 'vector', vectorSize = 100)
    model = LogisticRegression(featuresCol = 'vector', labelCol = 'Sentiment')
    pipeline = Pipeline(stages = [stage_1, stage_2, stage_3, model])
    print("Pipeline created", "\n")
    pipelineLRFit = pipeline.fit(training_set)
    print("Logit model created", "\n")

    # Building the model
    modelSummary = pipelineLRFit.stages[-1].summary
    modelSummary.accuracy

    pipelineLRFit.transform(training_set).show()
    return pipelineLRFit

appSchema = tp.StructType([
    tp.StructField("Artist", tp.StringType(), True),
    tp.StructField("Title", tp.StringType(), True),
    tp.StructField("URL", tp.StringType(), True),
    tp.StructField("SentimentText", tp.StringType(), True)
#    tp.StructField("@timestamp", tp.StringType(), True)
])

tweetStorage = spark.createDataFrame(sc.emptyRDD(), appSchema)

es_mapping = {
    "mappings": {
        "properties":
        {
            "Artist": {"type": "constant_keyword", "fielddata": True},
            "Title": {"type": "constant_keyword", "fielddata": True},
            "URL": {"type": "constant_keyword", "fielddata": True},
            "Tweet": {"type": "text", "fielddata": True},
            "Sentiment": {"type": "integer"}
#            "@timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
        }
    }
}

es = Elasticsearch(hosts = elastic_host)

response = es.indices.create(
    index = elastic_index,
    body = es_mapping,
    ignore = 400 # Ignore Error 400 Index already exists
)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("Index mapping SUCCESS: ", response['index'])

def mapinfo(batchDF, batchID):
    print("******** foreach ********", batchID)
    valueRdd = batchDF.rdd.map(lambda x: x[1])
    print(valueRdd, "\n")
    strList = valueRdd.map(lambda x: json.loads(x)).collect() #list of strings with json value
    print("\n", strList, "\n")
    tupleList = []
    for string in strList:
        splitStr = string.split('","')
        artistStr = splitStr[0].split('":"', 1)[1]
        titleStr = splitStr[1].split('":"', 1)[1]
        urlStr = splitStr[2].split('":"', 1)[1]
        tweetStr = splitStr[3].split('":"', 1)[1]

        tuple = (artistStr, titleStr, urlStr, tweetStr[:len(tweetStr) - 2])
        tupleList.append(tuple)
    tweetDF = spark.createDataFrame(data = tupleList, schema = appSchema)
    transformedDF = pipelineLogit.transform(tweetDF).select('Artist', 'Title', 'URL', 'SentimentText', 'prediction')
    transformedDF.show(truncate = False)

    transformedDF.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("append") \
    .option("es.mapping.id", "URL") \
    .option("es.nodes", elastic_host).save(elastic_index)

pipelineLogit = training()

print("Reading from Stream")
kafkaDataFrame = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()

print(type(kafkaDataFrame))
print(kafkaDataFrame)

df2 = kafkaDataFrame \
    .writeStream \
    .foreachBatch(mapinfo) \
    .start() \
    .awaitTermination()
