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

df_11 = df_10.drop('detail_age_type','detail_age', 'age_substitution_flag','age_recode_27', 'age_recode_12', 'infant_age_recode_22', 'icd_code_10th_revision','record_condition_20' , 'entity_condition_20', 'entity_condition_19', 'entity_condition_18', 'entity_condition_17', 'record_condition_19', 'record_condition_18', 'record_condition_16', 'record_condition_17','record_condition_15','record_condition_14','record_condition_13','record_condition_12','record_condition_11','record_condition_10','record_condition_9','record_condition_8','record_condition_7' )

# COMMAND ----------

df_11.dtypes

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

# COMMAND ----------

df_11.groupBy('record_condition_7').count().orderBy('count', ascending=False).show()