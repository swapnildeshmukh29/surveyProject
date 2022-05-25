from pyspark.sql.functions import *


class ReadDataUtils:
    def readSrcData(self,spark,path):
        df=spark.read.csv(path=path,
                          header=True,inferSchema=True)
        return df

    def writeTargetData(self,df,path):
        df.write.option("header","True").option("mode", "overwrite").csv(path)
        print("success")

    def dateExtract(self,spark,filename):

        df = spark.sparkContext.parallelize(filename).map(lambda x: x.split("-")).toDF()

        filedate = df.withColumn("date",to_date(regexp_extract("_6", "\\d{4}_\\d{1,2}_\\d{1,2}", 0), 'yyyy_MM_dd')).select("date")
        return filedate


