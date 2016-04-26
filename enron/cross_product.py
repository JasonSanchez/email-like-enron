import re
import math
from collections import Counter

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext


# Found the cosine similarity example
#  on stack overflow.
# we needed something that didn't require
# vectorizing the entire data set.

WORD = re.compile(r'\w+')


def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def text_to_vector(text):
    words = WORD.findall(text)
    return Counter(words)


if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("HillAndEnron")
    conf.set("spark.driver.maxResultSize", "10g")

    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    # path to hillary/enron avro
    enr = sqlContext.read.format(
        "com.databricks.spark.avro").load(
            "s3n://datasets-396316040607/enron_data/*.avro").repartition(16)
    hil = sqlContext.read.format(
        "com.databricks.spark.avro").load(
            "s3n://datasets-396316040607/hillary/*.avro").repartition(16)

    # register tables
    sqlContext.registerDataFrameAsTable(hil, "hillary")
    sqlContext.registerDataFrameAsTable(enr, "enron")

    # register udf
    sqlContext.registerFunction(
        "getCos", lambda x, y: get_cosine(text_to_vector(x), text_to_vector(y))
    )

    # do the cosine similarity on the text, get the top 1000 matches
    out = sqlContext.sql("SELECT h.author h_auth, e.author e_auth, "
                         "e.contents e_mail, h.contents h_mail, "
                         "getCos(e.contents, h.contents) as cos_sim "
                         "from hillary as h join enron as e order by cos_sim "
                         "desc limit 1000")

    # write back out to s3
    # write back out to s3
    out.save("s3n://datasets-396316040607/cos_sim/", format="json")
