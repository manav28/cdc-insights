# Databricks notebook source
import os
import pandas as pd
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql.functions import col, udf

# COMMAND ----------

year1 = (spark.read.csv(path='/FileStore/tables/2005_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year2 = (spark.read.csv(path='/FileStore/tables/2006_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year3 = (spark.read.csv(path='/FileStore/tables/2007_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year4 = (spark.read.csv(path='/FileStore/tables/2008_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year5 = (spark.read.csv(path='/FileStore/tables/2009_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year6 = (spark.read.csv(path='/FileStore/tables/2010_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year7 = (spark.read.csv(path='/FileStore/tables/2011_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year8 = (spark.read.csv(path='/FileStore/tables/2012_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year9 = (spark.read.csv(path='/FileStore/tables/2013_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year10 = (spark.read.csv(path='/FileStore/tables/2014_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())
year11 = (spark.read.csv(path='/FileStore/tables/2015_data.csv',header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache())

# COMMAND ----------

df = year1.union(year2)

# COMMAND ----------

df_2 = df.union(year3)

# COMMAND ----------

df_3 = df_2.union(year4)

# COMMAND ----------

df_4 = df_3.union(year5)

# COMMAND ----------

df_5 = df_4.union(year6)

# COMMAND ----------

df_6 = df_5.union(year7)

# COMMAND ----------

df_7 = df_6.union(year8)


# COMMAND ----------

df_8 = df_7.union(year9)


# COMMAND ----------

df_9 = df_8.union(year10)


# COMMAND ----------

df_10 = df_9.union(year11)

# COMMAND ----------

df_10.count()

# COMMAND ----------

df.count()

# COMMAND ----------

df_10.schema.names

# COMMAND ----------

df_11 = df_10.drop('detail_age_type','detail_age', 'age_substitution_flag','age_recode_27', 'age_recode_12', 'infant_age_recode_22', 'icd_code_10th_revision')

# COMMAND ----------

df_11.dtypes

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

# COMMAND ----------

df_11.groupBy('record_condition_5').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_12 = df_11.drop('record_condition_20' , 'entity_condition_20', 'entity_condition_19', 'entity_condition_18', 'entity_condition_17', 'record_condition_19', 'record_condition_18', 'record_condition_16', 'record_condition_17','record_condition_15','record_condition_14','record_condition_13','record_condition_12','record_condition_11','record_condition_10','record_condition_9','record_condition_8','record_condition_7' , 'record_condition_6', 'record_condition_5', 'entity_condition_16', 'entity_condition_15','entity_condition_14', 'entity_condition_13', 'entity_condition_12', 'entity_condition_11', 'entity_condition_10', 'entity_condition_9','entity_condition_8', 'entity_condition_7', 'entity_condition_6', 'entity_condition_5')

# COMMAND ----------

df_12.groupBy('current_data_year').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_12 = df_12.fillna({'place_of_injury_for_causes_w00_y34_except_y06_and_y07_' : 10})
df_12.groupBy('place_of_injury_for_causes_w00_y34_except_y06_and_y07_').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_12 = df_12.fillna({'130_infant_cause_recode' : 000})
df_12.groupBy('130_infant_cause_recode').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_12 = df_12.fillna({'activity_code' : 10})
df_12.groupBy('activity_code').count().orderBy('count', ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import *
df_12 = df_12.withColumn('method_of_disposition', regexp_replace('method_of_disposition', 'R' , 'O'))
df_12 = df_12.withColumn('method_of_disposition', regexp_replace('method_of_disposition', 'E' , 'O'))
df_12 = df_12.withColumn('method_of_disposition', regexp_replace('method_of_disposition', 'D' , 'O'))
df_12.groupBy('method_of_disposition').count().orderBy('count', ascending=False).show()

# COMMAND ----------

df_12 = df_12.fillna({'manner_of_death' : 8})
df_12.groupBy('manner_of_death').count().orderBy('count', ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import when 
df_12 = df_12.withColumn('Place_of_death_and_decedents_status', when(df_12['Place_of_death_and_decedents_status']== 9 , 7).otherwise(df['Place_of_death_and_decedents_status']))
df_12.groupBy('Place_of_death_and_decedents_status').count().orderBy('count', ascending=False).show()

# COMMAND ----------

display(df_12.select("sex","current_data_year").groupBy("current_data_year").agg(count("sex")))

# COMMAND ----------

from pyspark.sql import Row

df_12.registerTempTable("largeTable")
display(sqlContext.sql("select * from largeTable"))


# COMMAND ----------

# MAGIC %sql select current_data_year, sex, count("sex") from largeTable group by current_data_year, sex order by current_data_year, sex

# COMMAND ----------

# MAGIC %sql select current_data_year, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by current_data_year, 358_cause_recode order by current_data_year, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) and current_data_year IN (2015,2014,2013,2012,2011) group by current_data_year, 358_cause_recode order by current_data_year, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select sex, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by sex, 358_cause_recode order by sex, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, sex, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (429) group by race_recode_5, sex, 358_cause_recode order by race_recode_5, sex, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (435) group by race_recode_5, 358_cause_recode order by race_recode_5, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, sex 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (435) group by race_recode_5, sex, 358_cause_recode order by race_recode_5, sex, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select sex, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by sex, 358_cause_recode order by sex, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select age_recode_52, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by age_recode_52, 358_cause_recode order by count("358_cause_recode") desc, age_recode_52, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, age_recode_52, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) and age_recode_52 IN(23,24,25,26,27,28,29) group by current_data_year, age_recode_52, 358_cause_recode order by  current_data_year, count("358_cause_recode") desc, age_recode_52, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, 358_cause_recode , count("358_cause_recode") from largeTable where age_recode_52 IN (23, 24,25,26,27,28,29) and race_recode_5 = 2 group by race_recode_5, 358_cause_recode order by count("358_cause_recode") desc, race_recode_5, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select 358_cause_recode , count("358_cause_recode") from largeTable where age_recode_52 IN (23, 24,25,26,27,28,29) group by   358_cause_recode order by count("358_cause_recode") desc, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, age_recode_52, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) and age_recode_52 IN(28,29) group by current_data_year, age_recode_52, 358_cause_recode order by  current_data_year, count("358_cause_recode") desc, age_recode_52, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, place_of_injury_for_causes_w00_y34_except_y06_and_y07_, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (435) group by current_data_year, place_of_injury_for_causes_w00_y34_except_y06_and_y07_, 358_cause_recode order by current_data_year, count("358_cause_recode") desc, place_of_injury_for_causes_w00_y34_except_y06_and_y07_, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select month_of_death, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by month_of_death, 358_cause_recode order by count("358_cause_recode") desc, month_of_death, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select day_of_week_of_death, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by day_of_week_of_death, 358_cause_recode order by count("358_cause_recode") desc, day_of_week_of_death, 358_cause_recode

# COMMAND ----------

# MAGIC 
# MAGIC %sql select place_of_death_and_decedents_status, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by place_of_death_and_decedents_status, 358_cause_recode order by count("358_cause_recode") desc, place_of_death_and_decedents_status, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by race_recode_5, 358_cause_recode order by count("358_cause_recode") desc, race_recode_5, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select resident_status, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (407, 429,435,446) group by resident_status, 358_cause_recode order by count("358_cause_recode") desc, resident_status, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select age_recode_52, 358_cause_recode , count("age_recode_52") from largeTable group by age_recode_52, 358_cause_recode order by count("age_recode_52") desc, age_recode_52, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select month_of_death, 358_cause_recode , count("358_cause_recode") from largeTable group by month_of_death, 358_cause_recode order by count("358_cause_recode") desc, month_of_death, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select month_of_death, 39_cause_recode , count("39_cause_recode") from largeTable group by month_of_death, 39_cause_recode order by count("39_cause_recode") desc, month_of_death, 39_cause_recode

# COMMAND ----------

# MAGIC %sql select day_of_week_of_death, 39_cause_recode , count("39_cause_recode") from largeTable group by day_of_week_of_death, 39_cause_recode order by count("39_cause_recode") desc, day_of_week_of_death, 39_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_5, 358_cause_recode , count("358_cause_recode") from largeTable where age_recode_52 IN (23, 24,25,26,27,28,29) and 358_cause_recode in (435) group by   race_recode_5, 358_cause_recode order by count("358_cause_recode") desc, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select place_of_injury_for_causes_w00_y34_except_y06_and_y07_,358_cause_recode , count("358_cause_recode") from largeTable where age_recode_52 IN (23, 24,25,26,27,28,29) and 358_cause_recode in (407,429,435,446) group by place_of_injury_for_causes_w00_y34_except_y06_and_y07_, 358_cause_recode order by count("358_cause_recode") desc, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select race_recode_3, count("race_recode_3") from largeTable where 358_cause_recode IN (450,451) group by race_recode_3 order by count("358_cause_recode") desc

# COMMAND ----------

# MAGIC %sql select current_data_year, count("current_data_year") from largeTable where 358_cause_recode IN (420,443) group by current_data_year order by count("current_data_year") asc

# COMMAND ----------

# MAGIC %sql select race_recode_5, 358_cause_recode , count("358_cause_recode") from largeTable where 358_cause_recode IN (342,433) group by race_recode_5, 358_cause_recode order by count("358_cause_recode") desc, race_recode_5, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, count("358_cause_recode") from largeTable where manner_of_death = 2  group by  current_data_year ,358_cause_recode order by count("358_cause_recode") desc,  current_data_year, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select current_data_year, 358_cause_recode, count("358_cause_recode") from largeTable where manner_of_death = 2 and 358_cause_recode IN (424,
# MAGIC 425,
# MAGIC 426,
# MAGIC 427,
# MAGIC 428,
# MAGIC 429,
# MAGIC 430,
# MAGIC 431) group by  current_data_year ,358_cause_recode order by count("358_cause_recode") desc,  current_data_year, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select entity_condition_1, 358_cause_recode, count("358_cause_recode") from largeTable where manner_of_death = 2 and 358_cause_recode IN (424,
# MAGIC 425,
# MAGIC 426,
# MAGIC 427,
# MAGIC 428,
# MAGIC 429,
# MAGIC 430,
# MAGIC 431) group by entity_condition_1 ,358_cause_recode order by count("358_cause_recode") desc,  entity_condition_1, 358_cause_recode

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where manner_of_death = 2 and 358_cause_recode IN (424,
# MAGIC 425,
# MAGIC 426,
# MAGIC 427,
# MAGIC 428,
# MAGIC 429,
# MAGIC 430,
# MAGIC 431)

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode IN (407,
# MAGIC 429,
# MAGIC 435,
# MAGIC 446,
# MAGIC 450,
# MAGIC 451) 

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode IN (
# MAGIC 429)

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode IN (407,
# MAGIC 429,
# MAGIC 435,
# MAGIC 446,
# MAGIC 450,
# MAGIC 451) and age_recode_52 in (27,28,29)

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode IN (429) and age_recode_52 in (27,28,29)

# COMMAND ----------

# MAGIC %sql select current_data_year, count("358_cause_recode") from largeTable where 358_cause_recode IN (435) group by current_data_year order by current_data_year

# COMMAND ----------

# MAGIC %sql select race, count("race") from largeTable group by race order by count("race") desc

# COMMAND ----------

pt = sqlContext.sql("select  race,count(race) from largeTable group by race order by count(race) desc")

# COMMAND ----------

pt.show()

# COMMAND ----------

pt = pt.withColumnRenamed("count(race)","Total")

# COMMAND ----------


pt.registerTempTable("largeTable2")

# COMMAND ----------

sumtotal = sqlContext.sql("select(sum(Total)) from largeTable2")

# COMMAND ----------

sumtotal.show()

# COMMAND ----------

sumtotal = sumtotal.withColumnRenamed("sum(total)","sumtotal")

# COMMAND ----------

abc = sqlContext.sql("select current_data_year, month_of_death, count(358_cause_recode) as total from largeTable where manner_of_death = 2 group by current_data_year, month_of_death order by  current_data_year, month_of_death")

# COMMAND ----------

abc.registerTempTable('sf')

# COMMAND ----------

# MAGIC %sql select * from sf  

# COMMAND ----------


ls = sqlContext.sql("Select CONCAT( month_of_death, '-', current_data_year) as date,  total from sf")


# COMMAND ----------

# MAGIC %sql select 358_cause_recode,count("358_cause_recode") from largeTable where manner_of_death IN(2,3) and 358_cause_recode IN (435,
# MAGIC 429,
# MAGIC 428,
# MAGIC 441,
# MAGIC 425,
# MAGIC 436,
# MAGIC 430,
# MAGIC 431,
# MAGIC 451,
# MAGIC 440,
# MAGIC 427,
# MAGIC 426,
# MAGIC 434,
# MAGIC 438,
# MAGIC 433,
# MAGIC 437,
# MAGIC 439,
# MAGIC 450) group by 358_cause_recode

# COMMAND ----------

# MAGIC %sql select sex,count("358_cause_recode") from largeTable where 358_cause_recode==429  group by sex order by count("358_cause_recode") desc

# COMMAND ----------

# MAGIC %sql select age_recode_52,sex,count("358_cause_recode") from largeTable where 358_cause_recode==429  group by age_recode_52,sex order by count("358_cause_recode") desc

# COMMAND ----------

# MAGIC %sql select race_recode_3,current_data_year,count("358_cause_recode") from largeTable where 358_cause_recode==429 and sex=="M" and age_recode_52>38  group by current_data_year,race_recode_3 order by current_data_year desc

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode==435 and place_of_injury_for_causes_w00_y34_except_y06_and_y07_==0 

# COMMAND ----------

# MAGIC %sql select count("358_cause_recode") from largeTable where 358_cause_recode==435 

# COMMAND ----------

# MAGIC %sql select sex,count("358_cause_recode") from largeTable where 358_cause_recode IN (420,443)  group by  sex order by count("358_cause_recode") desc

# COMMAND ----------

# MAGIC %sql select current_data_year,count("358_cause_recode") from largeTable where 358_cause_recode IN (420,443) and age_recode_52 IN(31,32)  group by current_data_year order by current_data_year desc

# COMMAND ----------

# MAGIC %sql select current_data_year,count("358_cause_recode") from largeTable where 358_cause_recode =429 and marital_status=='M' and age_recode_52 in (35,36,37,38)    group by current_data_year order by current_data_year desc

# COMMAND ----------

# MAGIC %sql select current_data_year,count("358_cause_recode") from largeTable where 358_cause_recode =429  and age_recode_52 in (35,36,37,38)    group by current_data_year order by current_data_year desc