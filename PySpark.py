# Databricks notebook source
sch = "year int, month string, passengers int"

df_input = spark.readStream.option("header", True).schema(sch).csv('/FileStore/tables/')
display(df_input)

# COMMAND ----------

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

# MAGIC %md
# MAGIC
# MAGIC #### Removing duplicate values from dataframe

# COMMAND ----------

# create a dataframe with duplicate records

columns = ["language","users_count", "version"]
data = [("Java", "20000", "4.8"), ("Python", "100000", "3.7"), ("Scala", "3000", "2.1"), ("Java", "20000", "4.8"), ("Python", "15000", "3.9")]

df8 = spark.createDataFrame(data=data, schema=columns)
display(df8)

# COMMAND ----------

# method 1 (using distinct() )
# distinct() checks all the columns, removes only if all the columns are same

df8.distinct().show()

# COMMAND ----------

# method 2 (using dropDuplicates() )
# can check specfic columns too

df8.dropDuplicates(["language"]).show()

# COMMAND ----------

df8.dropDuplicates(["version"]).show()

# COMMAND ----------

df8.dropDuplicates(["language", "version"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using groupBy()

# COMMAND ----------

columns = ["ID", "Name", "Marks"]

data = [(1, "Vishal", 60), (2, "Utkarsh", 50), (1, "Vishal", 80), (3, "Sunil", 20), (2, "Utkarsh", 30)]

df9 = spark.createDataFrame(data=data, schema=columns)

display(df9)

# COMMAND ----------

df9.groupBy("ID").sum("Marks").show()

# COMMAND ----------

df9.groupBy("ID", "Name").sum("Marks").show()

# COMMAND ----------

df9.groupBy("ID", "Name").max("Marks").show()

# COMMAND ----------

df9.groupBy("ID", "Name").min("Marks").show()

# COMMAND ----------

df9.groupBy("ID", "Name").avg("Marks").show()

# COMMAND ----------

sampleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df10 = spark.createDataFrame(data=sampleData, schema = schema)
df10.show(truncate=False)

# COMMAND ----------

df10.groupBy("department").sum("salary").show()

# COMMAND ----------

# groupby multiple columns & agg

from pyspark.sql.functions import count

df10.groupBy("department", "state").agg(count("*").alias("count")).show()

# COMMAND ----------

df10.createOrReplaceTempView("emp_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select department, max(salary) from emp_temp group by department;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select department, sum(salary) from emp_temp group by department;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Save dataframe into csv file

# COMMAND ----------

df10.write.csv("dbfs:/user/hive/warehouse/employees.csv")

# COMMAND ----------

spark.read.format("csv").load("dbfs:/user/hive/warehouse/employees.csv").show()

# COMMAND ----------

df10.write.mode("append").csv("dbfs:/user/hive/warehouse/employees.csv")

# COMMAND ----------

spark.read.format("csv").load("dbfs:/user/hive/warehouse/employees.csv").show()

# COMMAND ----------

df10.write.mode("overwrite").csv("dbfs:/user/hive/warehouse/employees.csv")

# COMMAND ----------

spark.read.format("csv").load("dbfs:/user/hive/warehouse/employees.csv").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Merging dataframes

# COMMAND ----------

sampleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
emp_df1 = spark.createDataFrame(data=sampleData, schema = schema)
emp_df1.show(truncate=False)

# COMMAND ----------

sampleData = [
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["name","department","state","salary","age","bonus"]
emp_df2 = spark.createDataFrame(data=sampleData, schema = schema)
emp_df2.show(truncate=False)

# COMMAND ----------

# Method 1 (using union())
# to use union we should keep in mind that the schema of both the tables should be the same

emp_df1.union(emp_df2).show()

# COMMAND ----------

emp_df2.union(emp_df1).show()

# COMMAND ----------

emp_df1.union(emp_df1).show()

# COMMAND ----------

emp_df1.union(emp_df1).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### if-else condition

# COMMAND ----------

from pyspark.sql.functions import when, col

new_emp_df = emp_df1.withColumn("state_full_name", when(col("state")=="NY", "New York").when(col("state")=="CA", "California").otherwise("Unknown"))
new_emp_df.show()

# COMMAND ----------

# using select()

new_emp_df1 = emp_df1.select(col("employee_name"),col("department"),col("state"),when(col("state")=="NY", "New York").when(col("state")=="CA", "California").otherwise("Unknown").alias("full_state_name"))
new_emp_df1.show()

# COMMAND ----------

new_emp_df2 = emp_df1.select(col("*"),when(col("state")=="NY", "New York").when(col("state")=="CA", "California").otherwise("Unknown").alias("state_full_name"))
new_emp_df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Joining Dataframe

# COMMAND ----------

sampleData = [
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
emp_df2 = spark.createDataFrame(data=sampleData, schema = schema)
emp_df2.show(truncate=False)

# COMMAND ----------

sampleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
emp_df1 = spark.createDataFrame(data=sampleData, schema = schema)
emp_df1.show(truncate=False)

# COMMAND ----------

emp_df1.join(emp_df2, emp_df1.state == emp_df2.state, "inner").show()

# COMMAND ----------

emp_df1.join(emp_df2, emp_df1.state == emp_df2.state, "left").show()

# COMMAND ----------

emp_df1.join(emp_df2, emp_df1.state == emp_df2.state, "right").show()

# COMMAND ----------

emp_df1.alias("A").join(emp_df2.alias("B"), col("A.state") == col("B.state"), "inner").show()

# COMMAND ----------

emp_df1.alias("A").join(emp_df2.alias("B"), col("A.department") == col("B.department"), "inner").select(col("A.employee_name"), col("B.state")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Window Functions

# COMMAND ----------

all_emp_df = emp_df1.union(emp_df2)
all_emp_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

win = Window.partitionBy("state").orderBy("salary")
all_emp_df.withColumn("salary_row_number_by_state", row_number().over(win)).show()

# COMMAND ----------

win_rank = Window.partitionBy("state").orderBy(col("salary").desc())
all_emp_df.withColumn("salary_rank", rank().over(win_rank)).show()

# COMMAND ----------

win_dense_rank = Window.partitionBy("state").orderBy(col("salary").desc())
dense_rank_df = all_emp_df.withColumn("salary_rank", dense_rank().over(win_dense_rank))
dense_rank_df.show()

# COMMAND ----------

dense_rank_df.filter(col("salary_rank") == 2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using repartition()

# COMMAND ----------

df.write.format("parquet").mode("overwrite").save("/FileStore/tables/airlines")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`/FileStore/tables/airlines/part-00000-tid-3036573212398184268-fee37f5a-9fb0-474b-a19b-a5c77f60f50d-146-1.c000.snappy.parquet`;

# COMMAND ----------

df.repartition(3).write.format("parquet").mode("overwrite").save("/FileStore/tables/airlines")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from parquet.`dbfs:/FileStore/tables/airlines/part-00000-tid-872006459334780536-ddeb6ba9-f822-4fc9-bfb0-4dfc77e38fa7-150-1.c000.snappy.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from parquet.`dbfs:/FileStore/tables/airlines/part-00001-tid-872006459334780536-ddeb6ba9-f822-4fc9-bfb0-4dfc77e38fa7-151-1.c000.snappy.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from parquet.`dbfs:/FileStore/tables/airlines/part-00002-tid-872006459334780536-ddeb6ba9-f822-4fc9-bfb0-4dfc77e38fa7-152-1.c000.snappy.parquet`;

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("parquet").partitionBy("Active").mode("overwrite").save("/FileStore/tables/airlines1")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines1")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines1/Active=N")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines1/Active=Y")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using partitionBy()

# COMMAND ----------

sampleData = [
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000),
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
emp_df = spark.createDataFrame(data=sampleData, schema = schema)

# COMMAND ----------

display(emp_df)

# COMMAND ----------

emp_df.write.option("header", "true").mode("overwrite").csv("/FileStore/tables/emp_data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data")

# COMMAND ----------

emp_df.write.option("header", "true").partitionBy("state").mode("overwrite").csv("/FileStore/tables/emp_data")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/state=CA")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/state=NY")

# COMMAND ----------

# column / columns on which the partition is made is not save in the part files

spark.read.option("header", "true").format("csv").load("/FileStore/tables/emp_data/state=CA").show()


# COMMAND ----------

# column / columns on which the partition is made is not save in the part files

spark.read.option("header", "true").format("csv").load("/FileStore/tables/emp_data/state=NY").show()


# COMMAND ----------

emp_df.write.option("header", "true").partitionBy("department", "state").mode("overwrite").csv("/FileStore/tables/emp_data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/department=Finance/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/department=Marketing/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/department=Sales/")

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


