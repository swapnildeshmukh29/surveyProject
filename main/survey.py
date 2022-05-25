from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import Window

from common.readdatautil import ReadDataUtils

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Survey").getOrCreate()
    rdu=ReadDataUtils()

    # srcpath=r"C:\Users\user\PycharmProjects\surveydata\source\annual-enterprise-survey-financial-year-provisional_2022_05_21.csv"
    srcpath=r"C:\Data engineer\Assignment\Spark\Spark project\Project 2 SCD Type1 and Type2 Ingestion and handling Practice\survey data"
    trgtpath=r"C:\Users\user\PycharmProjects\surveydata\target\survey_target_data"
    trgtreadpath=r"C:\Users\user\PycharmProjects\surveydata\target\survey_data_2\*"
    trgtloadpath = r"C:\Users\user\PycharmProjects\surveydata\target\survey_data_2"

    #Reading Source data
    srcdata=rdu.readSrcData(spark,srcpath)
    # srcdata.select("Industry_code_NZSIOC","Variable_code").groupBy("Industry_code_NZSIOC").count().show()


    filename=["annual-enterprise-survey-financial-year-provisional_2022_05_21.csv"]

    filedate=rdu.dateExtract(filename=filename,spark=spark)
    # print(type(filedate))
    newsrcdata=srcdata.join(filedate)
    # newsrcdata.show()
    # newsrcdata.printSchema()

    #write target data
    # rdu.writeTargetData(newsrcdata,trgtloadpath)
    #Read target data
    targetdata1=rdu.readSrcData(spark,trgtreadpath)
    # targetdata1.show(truncate=True)
    newdata=srcdata.withColumn("date",to_date(lit("2022-05-22")))

    ud=newdata.union(targetdata1)
    myrank=Window.partitionBy("Industry_code_NZSIOC","Variable_code").orderBy(col("date").desc())
    ud1=ud.withColumn("rnk",rank().over(myrank)).filter(col("rnk")==1)
    ud1.show(500)

    scd2 = ud.withColumn("rnk", rank().over(myrank)).withColumn("to_date",lag("date",default="2999-12-31").over(myrank)).drop("rnk")
    scd2.show(600)



    """
    #Step 1 Reading Source data
    srcdata=rdu.readSrcData(spark,srcpath)
    #Step 2 creating an empty target data
    columns = StructType([StructField('Year',IntegerType(), True),
                          StructField('Industry_aggregation_NZSIOC',StringType(), True),
                          StructField('Industry_code_NZSIOC', StringType(), True),
                          StructField('Industry_name_NZSIOC', StringType(), True),
                          StructField('Units', StringType(), True),
                          StructField('Variable_code', StringType(), True),
                          StructField('Variable_name', StringType(), True),
                          StructField('Variable_category', StringType(), True),
                          StructField('Value', StringType(), True),
                          StructField('Industry_code_ANZSIC06',StringType(), True)])

    targetdata=spark.createDataFrame(data = [],schema = columns)
    # targetdata.show()

    #Step 3 rename source and target DFs columns
    srcdata = srcdata.withColumnRenamed("Year","Year_src").withColumnRenamed("Industry_aggregation_NZSIOC", "Industry_aggregation_NZSIOC_src") \
            .withColumnRenamed("Industry_code_NZSIOC", "Industry_code_NZSIOC_src") \
            .withColumnRenamed("Industry_name_NZSIOC", "Industry_name_NZSIOC_src") \
            .withColumnRenamed("Units", "Units_src").withColumnRenamed("Variable_code", "Variable_code_src")\
            .withColumnRenamed("Variable_name", "Variable_name_src").withColumnRenamed("Variable_category", "Variable_category_src") \
            .withColumnRenamed("Value", "Value_src").withColumnRenamed("Industry_code_ANZSIC06", "Industry_code_ANZSIC06_src")

    targetdata = targetdata.withColumnRenamed("Year", "Year_trgt").withColumnRenamed("Industry_aggregation_NZSIOC","Industry_aggregation_NZSIOC_trgt") \
        .withColumnRenamed("Industry_code_NZSIOC", "Industry_code_NZSIOC_trgt") \
        .withColumnRenamed("Industry_name_NZSIOC", "Industry_name_NZSIOC_trgt") \
        .withColumnRenamed("Units", "Units_trgt").withColumnRenamed("Variable_code", "Variable_code_trgt") \
        .withColumnRenamed("Variable_name", "Variable_name_trgt").withColumnRenamed("Variable_category","Variable_category_trgt") \
        .withColumnRenamed("Value", "Value_trgt").withColumnRenamed("Industry_code_ANZSIC06","Industry_code_ANZSIC06_trgt")


    #Step 4 joining source and target df.performing left outer join

    srcscd=srcdata.join(targetdata,srcdata.Year_src==targetdata.Year_trgt,"left")
    # srcscd.show()

    #Step 5 adding flag to the records

    scd_df=srcscd.withColumn('Flag',f.when((srcscd.Year_src !=srcscd.Year_trgt) | srcscd.Year_trgt.isNull(),'Y').otherwise('NA'))
    # scd_df.show()

    #Step 6 inserting records into target
    datainsert=scd_df.select(scd_df["Year_src"].alias("Year"),
                             scd_df["Industry_aggregation_NZSIOC_src"].alias("Industry_aggregation_NZSIOC"),
                             scd_df["Industry_code_NZSIOC_src"].alias("Industry_code_NZSIOC"),
                             scd_df["Industry_name_NZSIOC_src"].alias("Industry_name_NZSIOC"),
                             scd_df["Units_src"].alias("Units"),
                             scd_df["Variable_code_src"].alias("Variable_code"),
                             scd_df["Variable_name_src"].alias("Variable_name"),
                             scd_df["Variable_category_src"].alias("Variable_category"),
                             scd_df["Value_src"].alias("Value"),
                             scd_df["Industry_code_ANZSIC06_src"].alias("Industry_code_ANZSIC06"))
    # datainsert.show()
    
    # rdu.writeTargetData(datainsert,trgtpath)

    #Step7 import source and target data to track change
    src_data1=rdu.readSrcData(spark,srcpath)
    # src_data1.show()
    trgt_data1=rdu.readSrcData(spark,trgtpath)
    # trgt_data1.show()

    #combine source and target

    # Step 8 rename target DFs columns

    trgt_data1 = trgt_data1.withColumnRenamed("Year", "Year_trgt").withColumnRenamed("Industry_aggregation_NZSIOC","Industry_aggregation_NZSIOC_trgt") \
        .withColumnRenamed("Industry_code_NZSIOC", "Industry_code_NZSIOC_trgt") \
        .withColumnRenamed("Industry_name_NZSIOC", "Industry_name_NZSIOC_trgt") \
        .withColumnRenamed("Units", "Units_trgt").withColumnRenamed("Variable_code", "Variable_code_trgt") \
        .withColumnRenamed("Variable_name", "Variable_name_trgt").withColumnRenamed("Variable_category","Variable_category_trgt") \
        .withColumnRenamed("Value", "Value_trgt").withColumnRenamed("Industry_code_ANZSIC06","Industry_code_ANZSIC06_trgt")

    # Step 9 joining source and target df.performing left outer join

    srcscd1 = src_data1.join(trgt_data1, src_data1.Year == trgt_data1.Year_trgt, "left")
    # srcscd1.show()

    #Step 10 adding flag to records  new inserted or updated

    #Insert flag
    scd1_df=srcscd1.withColumn('I_flag',f.when((srcscd1.Year != srcscd1.Year_trgt) | srcscd1.Year_trgt.isNull(),'Y').otherwise("NA"))

    #udate Flag
    # cond1=(scd1_df.Year == scd1_df.Year_trgt)
    # cond2=(scd1_df.Industry_aggregation_NZSIOC != scd1_df.Industry_aggregation_NZSIOC_trgt)
    # cond3=scd1_df.Industry_code_NZSIOC !=scd1_df.Industry_code_NZSIOC_trgt

    scd2_df = scd1_df.withColumn('U_flag', f.when((scd1_df.Year == (scd1_df.Year_trgt)) & (scd1_df.Value != (scd1_df.Value_trgt)),'Y').otherwise("NA"))
    # scd2_df.show()

    #insertrecord
    scd_ins=scd2_df.select("Year","Industry_aggregation_NZSIOC","Industry_code_NZSIOC","Industry_name_NZSIOC","Units","Variable_code","Variable_name","Variable_category","Value","Industry_code_ANZSIC06").filter(scd2_df.I_flag=='Y')
    scd_ins.show()

    #update record
    scd_upd=scd2_df.select("Year","Industry_aggregation_NZSIOC","Industry_code_NZSIOC","Industry_name_NZSIOC","Units","Variable_code","Variable_name","Variable_category","Value","Industry_code_ANZSIC06").filter(scd2_df.U_flag=='Y')
    scd_upd.show()

    #Record override
    scd_over=scd2_df.select("Year","Industry_aggregation_NZSIOC","Industry_code_NZSIOC","Industry_name_NZSIOC","Units","Variable_code","Variable_name","Variable_category","Value","Industry_code_ANZSIC06").filter((scd2_df.U_flag!='Y') & (scd2_df.I_flag!='Y'))
    scd_over.show()

    #union all df
    df_final=scd_ins.unionAll(scd_upd).unionAll(scd_over)
    df_final.show()
    
    #Write data
    rdu.writeTargetData(df_final,trgtpath)
    """