# Databricks notebook source
# MAGIC %md
# MAGIC #My first Notebook
# MAGIC Documentation will go here

# COMMAND ----------

# Code de lecture CSV avec Spark
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
display(df)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Temporary view

# COMMAND ----------


df.createOrReplaceTempView("temp_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #Display Bar Graph

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT clarity, COUNT(clarity) as total
# MAGIC FROM temp_table
# MAGIC GROUP BY clarity
# MAGIC ORDER BY clarity

# COMMAND ----------

# MAGIC %md
# MAGIC ### #Create a Markdown Cell using bullet point
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # This is a header
# MAGIC - bullet 1
# MAGIC - bullet 2
# MAGIC - [example link](https://example.com)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the following cell to view the underlying files backing this table

# COMMAND ----------

files=dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled/")
display(files)
