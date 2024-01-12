# Databricks notebook source
# keeping the cluster alive


df_input = spark.readStream.format("delta").option("header", True).load('/FileStore/tables/airlines')
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

dbutils.fs.ls("/FileStore/tables/emp_data/department=Finance/state=CA")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_data/department=Finance/state=NY")

# COMMAND ----------

spark.read.option("header", "true").csv("/FileStore/tables/emp_data/department=Finance/state=CA").show()

# COMMAND ----------

spark.read.option("header", "true").csv("/FileStore/tables/emp_data/department=Finance/state=NY").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating UDF (User Defined Functions)

# COMMAND ----------

def convertcase(val):
    c = ""
    for i in val:
        if i.islower():
            c = c + i.upper()
        else:
            c = c + i.lower()
    
    return c

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

emp_df.select("employee_name", convertcase(col("employee_name"))).show()

# COMMAND ----------

from pyspark.sql.functions import udf

col_convert = udf(convertcase)

# COMMAND ----------

emp_df.select("employee_name", col_convert(col("employee_name")).alias("Case Converted")).show()

# COMMAND ----------

emp_df.select("employee_name", col_convert(col("employee_name")), col_convert(col("department")).alias("converted_department")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from airlines;

# COMMAND ----------

# use udf in sql

spark.udf.register("case_convert_sql", convertcase)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select case_convert_sql(Active) from airlines;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using cast()

# COMMAND ----------

sampleData = [("1", "Vishal", "100"), ("2", "Raj", "200"), ("3", "Batman", "300")]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data = sampleData, schema = schema)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("id", df.id.cast("int"))
df.show()
df.printSchema()

# COMMAND ----------

df = df.withColumn("id", df.id.cast("int")).withColumn("salary", df.salary.cast("int"))
df.show()
df.printSchema()

# COMMAND ----------

sampleData = [("1", "Vishal", "100"), ("2", "Raj", "200"), ("3", "Batman", "300")]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data = sampleData, schema = schema)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df = df.select(col("id").cast("int").alias("ID"), col("name").alias("Name"), col("salary").cast("int").alias("Salary"))

df.show()

df.printSchema()

# COMMAND ----------

sampleData = [("1", "Vishal", "100"), ("2", "Raj", "200"), ("3", "Batman", "300")]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data = sampleData, schema = schema)
df.show()

# COMMAND ----------

df = df.selectExpr('cast(id as int)', 'cast(salary as int)')

df.printSchema()

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Handling Null Values

# COMMAND ----------

sampleData = [(1, "Vishal", 100), (2, "Raj", None), (3, None, 300), (4, "Superman", None)]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data = sampleData, schema = schema)

df.show()

# COMMAND ----------

df = df.na.fill("Batman", "name")
df.show()

# COMMAND ----------

df.na.fill(150, "salary").show()

# COMMAND ----------

from pyspark.sql.functions import avg

avg_value = df.selectExpr("avg(salary) as avg_salary").collect()[0]['avg_salary']

df.na.fill(avg_value, ["salary"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Pivoting the dataframe

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

# in pyspark pivoting is not possible without aggregation

df_agg = emp_df.groupBy("department", "state").sum("salary", "bonus")
display(df_agg)

# COMMAND ----------

df_pivot = emp_df.groupBy("department").pivot("state").sum("salary", "bonus")
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Types of mode while reading
# MAGIC

# COMMAND ----------

# permissive -> sets the corrupted record to null

# dropmalformed -> drops the row of the corrupted record

# failfast -> throws an exception

# badRecordsPath -> Saves the bad records to the specified path

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### dbutils commands

# COMMAND ----------

# dbutils.fs.cp() -> copy file to the given location

# dbutils.fs.head() -> returns upto to the first 'maxBytes' of the data

# dbutils.fs.ls() -> lists all the directories

# dbutils.fs.mkdirs() -> creates directory if doesn't exist

# dbutils.fs.mv() -> moves a file or directory from one location to another

# dbutils.fs.put() -> writes the given string to a file

# dbutils.fs.rm() -> removes file or directory

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### insertInto()

# COMMAND ----------

sampleData = [("1", "Vishal", "100"), ("2", "Raj", "200"), ("3", "Batman", "300")]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data = sampleData, schema = schema)
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table emp(
# MAGIC   id int,
# MAGIC   name varchar(20),
# MAGIC   salary int
# MAGIC );

# COMMAND ----------

df.write.insertInto("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from emp;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### collect() vs select()

# COMMAND ----------

# collect() method retrieves all the data from DataFrame as an array in the driver program. 
# This can lead to memory issues if the DataFrame is huge.
 
df.collect()

# select() method is used to select one or more columns from a DataFrame. 
# It returns a new DataFrame with the selected columns.
 
df_new = df.select("name", "salary")


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create_df").getOrCreate()

data = [("John", 25), ("Lisa", 30), ("Charlie", 35), ("Mike", 40), ("Jude", 45)]
columns = ["name", "age"]

df = spark.createDataFrame(data=data, schema=columns)
df


# COMMAND ----------

df.collect()

# COMMAND ----------

for i in df.collect():
    print(f"Name is {i.name} and age is {i.age}")

# COMMAND ----------

df.collect()[0:2]

# COMMAND ----------

df.select()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

df.select("name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Working with json files

# COMMAND ----------

spark.read.option("header", "true").text("/FileStore/tables/ny_city.json").show()

# COMMAND ----------

ny_city = spark.read.option("header", "true").option("multiline", "true").json("/FileStore/tables/ny_city.json")
display(ny_city)

# COMMAND ----------

ny_city.select("collision_id", "contributing_factor_vehicle_1").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view ny_city_temp_view using json options("multiline" True, path '/FileStore/tables/ny_city.json')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ny_city_temp_view;

# COMMAND ----------

df.write.mode("overwrite").save("/FileStore/tables/ny_city_sink")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/ny_city_sink")

# getting saved in parquet format as it's by default 

# COMMAND ----------

df.write.format("json").mode("overwrite").save("/FileStore/tables/ny_city_sink_new")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/ny_city_sink_new")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### _SUCCESS , _committed, _started

# COMMAND ----------

# These 3 files are created by DBFS itself in the DBFS directories

#  _started -> It depicts that the process has been stated
# _committed -> It depicts how many files got created
# _SUCCESS -> It depicts that the transfer / loading happended successfully

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Working with XML files 

# COMMAND ----------

# MAGIC %fs head '/FileStore/tables/la_city.xml'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Filling null values

# COMMAND ----------

# Importing necessary libraries
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession

# Creating SparkSession object
spark = SparkSession.builder.appName('null_values').getOrCreate()

# Defining the schema of the DataFrame
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True)
])

# Creating the DataFrame with some null values
data = [(1, 'Alice', 25, 'Female'),
        (2, None, 30, 'Male'),
        (3, 'Bob', None, 'Male'),
        (4, 'Claire', 28, None),
        (5, 'David', 35, 'Male')
       ]
df = spark.createDataFrame(data, schema=schema)
df.show()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.fillna(27).show()

# COMMAND ----------

df.fillna("27").show()

# COMMAND ----------

df.fillna("Vishal").show()

# COMMAND ----------

df.fillna("Vishal", subset="name").show()

# COMMAND ----------

df.fillna(23, subset="age").show()

# COMMAND ----------

df.fillna("Male", subset="gender").where(df.id =="4").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Map Transformation

# COMMAND ----------

# map() trasformation is used to apply any comlex operations like adding a col, updating a column, transforming the data etc.

# The no of input rows == no of output rows

# df needs to be converted to rdd

# narrow transformation bcz shuffling doesn't happen

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.fillna(23, subset="age")
df.show()

# COMMAND ----------

mapped_df = df.rdd.map(lambda x : (x[0], x[1], x[2]+5, x[3]))
mapped_df.collect()

# COMMAND ----------

new_df = mapped_df.toDF([ "id", "name", "age", "gender"])
new_df.show()

# COMMAND ----------

def update_age(x):
    return x.id, x.name, x.age+5, x.gender

# COMMAND ----------

mapped_df = df.rdd.map(lambda x :update_age(x))
mapped_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### cache() & persist()

# COMMAND ----------

# cache() is a shorthand for persist(StorageLevel.MEMORY_ONLY)
# persist() allows for more control of the storage level and parameters, such as MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY,
# and the option to set replication and serialization
# cache() and persist() both persist the RDD in memory for faster access, but persist() allows for more options and customization


# COMMAND ----------

# Example using cache() function

mapped_df = df.rdd.map(lambda x :update_age(x))
mapped_df.cache()
mapped_df.count()

# COMMAND ----------

# Example using persist() function

from pyspark import StorageLevel

mapped_df = df.rdd.map(lambda x :update_age(x))
mapped_df.persist(StorageLevel.DISK_ONLY)
mapped_df.count()

# COMMAND ----------

mapped_df = df.rdd.map(lambda x :update_age(x))
mapped_df.persist(StorageLevel.MEMORY_ONLY)
mapped_df.count()

# COMMAND ----------

mapped_df = df.rdd.map(lambda x :update_age(x))
mapped_df.persist(StorageLevel.MEMORY_AND_DISK)
mapped_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Connect to blob storage using SAS token

# COMMAND ----------

# spark.conf.set(f"fs.azure.sas.{container}.{storageAccountName}.blob.core.windows.net", saskey)

# wasbs://{container}@{storageAccountName}.blob.core.windows.net

# COMMAND ----------

spark.conf.set(f"fs.azure.sas.upstream.vs17storage.blob.core.windows.net", "saskey")

# COMMAND ----------

dbutils.fs.ls("wasbs://upstream@vs17storage.blob.core.windows.net")

# COMMAND ----------

ny_city = spark.read.format("json").option("header", "true").option("multiline", "true").load("wasbs://upstream@vs17storage.blob.core.windows.net/ny-city.json")
display(ny_city)

# COMMAND ----------

ny_city_transformed = ny_city.fillna("Unknown")
display(ny_city_transformed)

# COMMAND ----------

ny_city_transformed.write.mode("overwrite").format("parquet").save("wasbs://upstream@vs17storage.blob.core.windows.net/output")

# COMMAND ----------

ny_city_transformed.write.mode("overwrite").format("parquet").save("wasbs://downstream@vs17storage.blob.core.windows.net/output")

# COMMAND ----------

display(spark.read.format("parquet").load("wasbs://downstream@vs17storage.blob.core.windows.net/output/part-00000-tid-303044258587521285-a4aad4ba-68b9-4434-a61b-1a5f2c364a16-8-1.c000.snappy.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Connecting to blob using access key

# COMMAND ----------

# config key -> fs.azure.account.key.{storage-account-name}.blob.core.windows.net


dbutils.fs.mount(
    source = "wasbs://{container}@{storageAccountName}.blob.core.windows.net/",
    mount_point = "mnt/raw_blob",
    extra_configs = Map("<conf-key>" -> dbutils.secrets.get(
        scope = "<scope-name>",
        key = "<key-name>")
    ) 
)

# if no scope and key remove map() & secrets

dbutils.fs.mount(
    source = "wasbs://{container}@{storageAccountName}.blob.core.windows.net/",
    mount_point = "mnt/raw_blob",
    extra_configs = {"<conf-key>":"<access_key>"}
)

# COMMAND ----------

dbutils.fs.unmount()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Working with APIs
# MAGIC
# MAGIC

# COMMAND ----------

import requests,json

def rest_api(url):
    try:
        response = requests.get(url)
        data = response.json()
        return data
    except Exception as e:
        print(f"Failed and got error : {e}")
        

# COMMAND ----------

url = "https://pokeapi.co/api/v2/pokemon"
data = rest_api(url)
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating dynamic schema

# COMMAND ----------

display(ny_city)

# COMMAND ----------

ny_city.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType

list_schema = ny_city.dtypes
fields = []

for i in range(len(list_schema)):
    if(list_schema[i][1]=="int"):
        list_schema[i] = (list_schema[i][0], "interger")
        
for i in list_schema:
    if list_schema[i][0] == "location":
        continue
    else:
        fields.append({"metadata":{}, "name":i[0], "nullable":True, "type":i[1]})
    
final_schema = StructType.fromJson({"fields":fields, "type":"struct"})
print(final_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Dynamic delimiter for csv data

# COMMAND ----------

import re

def get_delimiter(path):
    try:
        headerlist = sc.textFile(path).take(1)
        header_str = "".join(headerlist)
        result = re.search("(,|;|\\|)", header_str)
        return result.group()
    except Exception as e:
        print(f"Error Occured : {e}")

# COMMAND ----------

get_delimiter("/FileStore/tables/flights1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating table with dynamic schema

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists testing;

# COMMAND ----------

def delta_table(tablename, schema):
    try:
        spark.sql(f"""CREATE TABLE IF NOT EXISTS testing.{tablename} ({schema}) USING DELTA LOCATION '/FIleStore/tables/delta/{tablename}' """)
        print(f"Table created ; {tablename}")
    except expection as e:
        print(f"Error occured: {e}")

# COMMAND ----------

ddl_schema = [
    {
        "tablename" : "testing1",
        "schema" : "ID int, Age int"
    },
    {
        "tablename" : "testing2",
        "schema" : "ID int, Name string"
    },
    {
        "tablename" : "testing3",
        "schema" : "ID int, Age int, Email varchar(50)"
    }
]

# COMMAND ----------

dbutils.fs.put('/FileStore/tables/ddl_schema.json', str(ddl_schema), True)

# COMMAND ----------

def main():
    try:
        df = spark.read.json("/FileStore/tables/ddl_schema.json")
        for i in df.collect():
            delta_table(i.tablename, i.schema)
    except expection as e:
        print(f"Error occured: {e}")

main()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc table testing.testing1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc table testing.testing2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc table extended testing.testing3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating demo data using farsante

# COMMAND ----------

!pip install farsante

# COMMAND ----------

import farsante
help(farsante)

# COMMAND ----------

import farsante as f

df = f.quick_pyspark_df(["first_name", "last_name"], 100)
df.show()

# COMMAND ----------

from mimesis import Business
import farsante as f
b = Business("en")
df = f.pyspark_df([b.price, b.price_in_btc], 500)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Converting parquet to delta

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines")

# COMMAND ----------

from delta.tables import *

DeltaTable.convertToDelta(spark, "parquet.`/FileStore/tables/airlines`")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/airlines")

# COMMAND ----------

DeltaTable.isDeltaTable(spark, "/FileStore/tables/airlines")

# COMMAND ----------

# IF the data is partitioned pass the column name and the datatype as thrid parameter

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Having limited rows saved in partfile

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("/FileStore/tables/table1")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/table1")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").option("maxRecordsPerFile", 50).format("delta").save("/FileStore/tables/table2")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/table2")

# COMMAND ----------

len(dbutils.fs.ls("/FileStore/tables/table2"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Delta Table

# COMMAND ----------

# A delta table is a versioned table that stores data in Parquet format. Delta table provides several advantages over other storage formats, including ACID transactions, data versioning, and automatic schema enforcement. Delta table is optimized for building data pipelines with Apache Spark, providing high performance and efficient write operations to support data ingestion and ETL workloads. Unlike other sources, delta table allows for safe, concurrent writes to a table while maintaining consistency and providing high data integrity. Delta table also provides several APIs and optimizations to improve query performance and reduce the cost of running analytics workloads on large datasets. It can be used for both batch and real time processing


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create database delta_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use delta_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table parquet_table(
# MAGIC   id int,
# MAGIC   name char(20)
# MAGIC ) using parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table delta_table(
# MAGIC   id int,
# MAGIC   name char(20)
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended parquet_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into parquet_table values (1, "Vishal"), (2, "Shyam"), (3, "Ram"), (4,"Sita");
# MAGIC
# MAGIC insert into delta_table values (1, "Vishal"), (2, "Shyam"), (3, "Ram"), (4,"Sita");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from parquet_table where id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta_table where id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update parquet_table set name = "Hanuman" where id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update delta_table set name = "Hanuman" where id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table delta_table2(
# MAGIC   id int,
# MAGIC   name char(20)
# MAGIC ) using delta;
# MAGIC
# MAGIC insert into delta_table2 values (5, "Utkarsh"), (6, "Priya"), (7, "Disha"), (4,"Sita");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into delta_table t1
# MAGIC using delta_table2 t2
# MAGIC on t1.id = t2.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc history delta_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table version as of 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table timestamp as of '2024-01-08T14:52:48Z';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC restore table delta_table to version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_table;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Updating multiple rows in delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table delta_table(
# MAGIC   id int,
# MAGIC   name char(20)
# MAGIC ) using delta;
# MAGIC
# MAGIC insert into delta_table values (1, "Vishal"), (2, "Shyam"), (3, "Ram"), (4,"Sita"), (5, "Utkarsh"), (6, "Priya"), (7, "Disha"), (8,"Geeta");

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()

data = [(1, 'John'), (2, 'Mary'), (3, 'Jane')]
schema = StructType([StructField('ID', IntegerType(), True), StructField('Name', StringType(), True)])
df = spark.createDataFrame(data=data, schema=schema)

display(df)


# COMMAND ----------

df.createOrReplaceTempView("temp_person");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into delta_table t1
# MAGIC using temp_person t2
# MAGIC on t1.id = t2.ID
# MAGIC when matched then update set t1.name = t2.name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Check if file is empty or not

# COMMAND ----------

display(df)

# COMMAND ----------

def check_df_size(df):
    try:
        if df.count() == 0:
            print("df is empty")
        else:
            print(f"Length of dataframe is {df.count()}")
    except Exception as e:
        print(f"Error : {e}")

check_df_size(df)

# COMMAND ----------

len(df.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### input_file_name()

# COMMAND ----------

from pyspark.sql.functions import input_file_name, split, to_date

df = spark.read.option("header", True).format("parquet").load("/FileStore/tables/airlines1/Active=Y/part-00000-tid-104251474602577597-aa83316a-7d20-49a7-be0e-716bc16c741e-167-3.c000.snappy.parquet")
display(df)

# COMMAND ----------

df1 = df.withColumn("table", split(input_file_name(),"/")[2])
df1 = df1.withColumn("directory", split(input_file_name(),"/")[3])
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Validate table using Delta Lake

# COMMAND ----------

delta_df = spark.createDataFrame([(1, "Praveen"), (2, "Sahil"), (3, "Ram"), (4,"Sita"), (5, "Utkarsh"), (6, "Priya"), (7, "Disha"), (8,"Geeta")], ["ID", "Name"])
display(delta_df)

# COMMAND ----------

delta_df.write.mode("overwrite").format("delta").save("/FileStore/tables/delta_table")

# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql.functions import col

delta_table = DeltaTable.forPath(spark,"/FileStore/tables/delta_table")
display(delta_table.history())

# COMMAND ----------

display(delta_table.history().select("operationMetrics.numOutputRows").where(col("operation")=="WRITE").orderBy(col("timestamp").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Broadcasting 

# COMMAND ----------

# Broadcasting is a mechanism in Spark that allows a small amount of data called the broadcast variable to be sent from the driver node to all executor nodes so that any task running on those nodes can access that data. This is useful in situations where a large Data Frame or RDD needs to be joined with a relatively small lookup table. Broadcasting the lookup table can help reduce the amount of data shuffled between executor nodes, which in turn can improve performance and reduce network IO. However, broadcasting should be used judiciously as it can cause memory overload if the size of the broadcast variable is too large.


# COMMAND ----------

from pyspark.sql.functions import broadcast


# Sample data for a large DataFrame (consider this as read-only data)
large_data = [("Alice", 25), ("Bob", 30), ("Charlie", 22), ("David", 35)]
large_df = spark.createDataFrame(large_data, ["Name", "Age"])

# Sample data for a small DataFrame (consider this as a broadcast variable)
small_data = [("Alice", "Engineering"), ("Bob", "Marketing"), ("Charlie", "Finance")]
small_df = spark.createDataFrame(small_data, ["Name", "Department"])

# Broadcast the small DataFrame
broadcast_small_df = broadcast(small_df)

# Perform a join operation with the large DataFrame and the broadcasted small DataFrame
result_df = large_df.join(broadcast_small_df, "Name", "left_outer")

# Show the result
result_df.show()



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Accumulator

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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


