from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp
#import databricks.koalas as ks
#from pyspark.sql import HiveContext
#hc = HiveContext(sc)
from datetime import date
from datetime import datetime,timedelta
#today = date.today()-timedelta(days=1)
today = date.today()
d1 = today.strftime("%Y%m%d")
#warehouse_location = abspath("/user/hive/warehouse")
filedaily="Santander_Train_product_data_"
formatt=".csv"
nifitempdirectory="/nifitemp/"
sdfgf=nifitempdirectory+filedaily+d1+formatt
spark = SparkSession \
    .builder \
    .appName("rawToOptimizedDataLoading......") \
    .getOrCreate()
    #.config("spark.sql.warehouse.dir", warehouse_location) \
    #.getOrCreate()
    #.enableHiveSupport() \
    #.getOrCreate()
#Patient = spark.read.option("header", "true").csv("/RawData/Patient_02052020.csv")
#satender=spark.sql("SELECT * FROM nbopoc.rawSatenderData")
Patient = spark.read.option("header", "true").csv("/nifitemp/Santander_Train_product_data_"+d1+".csv")
#satender = spark.read.option("header", "true").csv("/RawDataLayer/satenderData/model/")
#satender = ks.read_csv('/clouderaDemo/satenderRawDataSpark/*.csv')
#Patient.show()
Patient.createOrReplaceTempView("Patient")
Patient.write.format("parquet").save("/opdata/partcol="+d1+"/")
#Patient1 = Patient.withColumn("PATIENTNAME", regexp_replace(col("PATIENTNAME"), "[^a-zA-Z0-9_-]", "****masked****"))
#Patient1.repartition(1).write.mode('overwrite').parquet('/OptimisedLayer')
#satender.repartition(1).write.mode('overwrite').parquet('/optimizedSatenderData/satenderdata/')
#satender.write.parquet("/clouderaDemo/satenderOptimizedData/temp.parquet", mode='overwrite')
#satender.to_parquet('/clouderaDemo/satenderOptimizedData/temp22.parquet')
#spark.sql("alter table nbopoc.nifitempdata add partition(partcol='${d1}') location '/opdata/'${d1}'")
#spark.sql("insert into nbopoc.nifitempdata partition(partcol="+d1+") select * from Patient")
#spark.sql("select * from Patient")
#Patient.write.mode(Overwrite).save(/opdata/)
