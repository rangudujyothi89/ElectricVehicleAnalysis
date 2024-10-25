# Databricks notebook source
df = spark.read.option("multiline",True).option("inferSchema",True).option("header",True).json("/FileStore/tables/ElectricVehiclePopulationData.json")
df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, explode

df_columns =df.select(explode(col("meta.view.columns.fieldName"))).alias("fieldNames")
display(df_columns)
df_columns.printSchema()


# COMMAND ----------

import pyspark.sql.functions as f

df_data = df.select(explode(col("data")))
display(df_data)
# Define the number of elements in each array (should be the same for all rows)
num_elements = len(df_data.first()["col"])
display(num_elements)

# Dynamically split the array into individual columns in the new DataFrame
new_columns = [f.col("col")[i].alias(f"col_{i}") for i in range(num_elements)]
target_df = df_data.select(*new_columns)
display(target_df)

# COMMAND ----------

#df_data.columns = df_columns.rdd.values
from pyspark.sql.types import StructType, StructField, StringType

def build_schema_from_column(df, column_name):
    """Builds a PySpark schema from the unique values in a specified column."""

    unique_values = df.select(column_name).rdd.flatMap(lambda x: x).collect()
    fields = [StructField(value, StringType(), True) for value in unique_values]
    return StructType(fields)

schema = build_schema_from_column(df_columns, "col")    
display(schema)

# COMMAND ----------

df_actualdata =  spark.createDataFrame(data=target_df.rdd,schema=schema)
display(df_actualdata)
