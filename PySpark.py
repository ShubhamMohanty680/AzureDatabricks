# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Reading file from storage

# COMMAND ----------

df = spark.read.format("delta") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("sep", ",") \
    .load("dbfs:/user/hive/warehouse/airlines")

display(df)

# COMMAND ----------

# check schema

df.printSchema()

# COMMAND ----------

# create a view

df.createOrReplaceTempView("airlines_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from airlines_temp_view;

# COMMAND ----------

# presist the dataframe in permanent storage

df.write.format("parquet").saveAsTable("airlines_parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from airlines_parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Renaming the columns

# COMMAND ----------

# method 1 (using withColumnRenamed() )

#renaming single column

df1 = df.withColumnRenamed("Name", "Airline Name")
df1.show()

# COMMAND ----------

# renaming multiple columns

df2 = df.withColumnRenamed("Name", "Airline Name").withColumnRenamed("Active", "Status")

df2.show()

# COMMAND ----------

# method 2 (using selectExpr() function)
# selectExpr() select only the specified columns. Throws error while working with column names having space
df3 = df.selectExpr("Name", "IATA")
df3.show()

# COMMAND ----------

df3 = df.selectExpr("Name as Airlines Name", "IATA")
df3.show()

# COMMAND ----------

df3 = df.selectExpr("Name as Airlines_Name", "IATA")
df3.show()

# COMMAND ----------

# method 3 (using select() & col() )
from pyspark.sql.functions import col

df4 = df.select(col("Name"), col("IATA"))
df4.show(5)

# COMMAND ----------

df4 = df.select(col("Name").alias("Airline Name"), col("IATA"), col("Active").alias("Status"))
df4.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Adding new columns to the dataframe

# COMMAND ----------

# method 1 (using lit() )

from pyspark.sql.functions import lit

df5 = df.withColumn("Airplane Model", lit("Airbus A320"))
df5.show(5)

# COMMAND ----------

# method 2

for i in range(df.select("Name").count()):
    pass
df6 = df.withColumn("X", lit(i))
df6 = df6.withColumn("Y", lit(i//2))
df6 = df6.withColumn("Prod", lit(col("X")*col("Y")))
df6.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Filtering from dataframe

# COMMAND ----------

df.filter(df.Name == "135 Airways").show()

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("Name") == "135 Airways").show()

# COMMAND ----------

df.filter((col("Name")=="135 Airways") | (col("Country")=="Russia")).show()

# COMMAND ----------

df.filter((col("Name")=="135 Airways") & (col("Country")=="Russia")).show()

# COMMAND ----------

df.filter(col("Country") != "Russia").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Sorting the dataframe

# COMMAND ----------

# method 1 (using sort() function )

df.sort(df.Name).show()

# COMMAND ----------

df.sort(col("Name").desc()).show()

# COMMAND ----------

# method2 (using orderBy() )

df.orderBy(df.Name).show()

# COMMAND ----------

df.orderBy(col("Name").desc()).show()

# COMMAND ----------

# sorting via multiple columns

df.sort(col("Name"), col("Country")).show(10)

# COMMAND ----------

df.sort(col("Country"), col("Name")).show(10)

# COMMAND ----------

df.sort(col("Country"), col("Name").desc()).show(50)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


