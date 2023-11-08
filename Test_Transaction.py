# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import requests
import pandas as pd
path_source = 'https://personalygblob.blob.core.windows.net/csvdailysource/transaction.csv?sp=r&st=2023-11-06T13:28:50Z&se=2023-11-06T21:28:50Z&spr=https&sv=2022-11-02&sr=b&sig=w%2BRLKAbx%2BXISzxlnDUeSG3buqM6FcYk5xZQqYSLxzWM%3D'

# COMMAND ----------

# MAGIC %md
# MAGIC ## load df to temp

# COMMAND ----------

df = pd.read_csv(path_source, sep ='|')
# Initialize a Spark session
spark = SparkSession.builder.appName("PandasToDatabricks").getOrCreate()

# Assuming 'df' is your Pandas DataFrame
# Convert the Pandas DataFrame to a PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Save the PySpark DataFrame as a temporary table in Databricks
spark_df.createOrReplaceTempView("transac_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS mydb;
# MAGIC USE mydb;
# MAGIC
# MAGIC -- Create a source table
# MAGIC CREATE TABLE IF NOT EXISTS mydb.transaction_source (
# MAGIC     transactionId STRING,
# MAGIC     custId BIGINT,
# MAGIC     transactionDate DATE,
# MAGIC     productSold STRING,
# MAGIC     unitsSold INT
# MAGIC );
# MAGIC
# MAGIC -- Insert data from the DataFrame into the table
# MAGIC INSERT INTO mydb.transaction_source SELECT * FROM transac_temp
# MAGIC where transactionid not in (select DISTINCT transactionId from mydb.transaction_source);

# COMMAND ----------

# MAGIC %md
# MAGIC ##transformation and insert into target table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a source table
# MAGIC CREATE TABLE IF NOT EXISTS mydb.transaction_target (
# MAGIC     customer_id int,
# MAGIC     favourite_product STRING,
# MAGIC     longest_streak smallint
# MAGIC );
# MAGIC
# MAGIC -- Insert data from the DataFrame into the table
# MAGIC INSERT INTO mydb.transaction_target SELECT * FROM (
# MAGIC with date_prev as (
# MAGIC SELECT custId,transactionDate,lag(transactionDate) OVER (partition by custId ORDER BY transactionDate) as prev_day, Abs(datediff(Day, transactionDate, lag(transactionDate) OVER (partition by custId ORDER BY transactionDate))) as days,
# MAGIC case when Abs(datediff(Day, transactionDate, lag(transactionDate) OVER (partition by custId ORDER BY transactionDate))) = 1 and lead(Abs(datediff(Day, transactionDate, lag(transactionDate) OVER (partition by custId ORDER BY transactionDate)))) OVER (partition by custId ORDER BY transactionDate) = 1 then 1 else 0 end as con
# MAGIC FROM  mydb.transaction_source 
# MAGIC group by custId,transactionDate
# MAGIC order by custId,transactionDate asc
# MAGIC ),
# MAGIC continuous_days as (
# MAGIC select custId,transactionDate,case when days = 1 then sum(con)+days+1 else 0 end as dayyy 
# MAGIC from date_prev
# MAGIC group by custId,days,transactionDate
# MAGIC order by custId,transactionDate asc),
# MAGIC fav_pro as (
# MAGIC SELECT custId,productSold,sum(unitsSold) as total_sold, row_number() OVER (PARTITION BY custId order by sum(unitsSold) desc) as Ranked FROM  mydb.transaction_source
# MAGIC group by custId,productSold order by custId,total_sold desc)
# MAGIC
# MAGIC select c.custId as customer_id,productSold as favourite_product, case when max(dayyy) = 0 then 1 else max(dayyy) end as longest_streak 
# MAGIC from continuous_days c join fav_pro f on c.custId=f.custId 
# MAGIC where Ranked = 1 group by c.custId,productSold) as source
# MAGIC
# MAGIC where customer_id not in (select DISTINCT customer_Id from mydb.transaction_target);
