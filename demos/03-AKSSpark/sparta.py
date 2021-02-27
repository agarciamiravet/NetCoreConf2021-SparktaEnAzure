import pyspark
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
url = "https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv"

from pyspark import SparkFiles
sc.addFile(url)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv(SparkFiles.get("adult_data.csv"), header=True, inferSchema= True)

df.printSchema()

df.show(5, truncate = False)