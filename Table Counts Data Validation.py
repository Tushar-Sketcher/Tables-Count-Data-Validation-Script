# Databricks notebook source
# MAGIC %md
# MAGIC #### User Inputs:
# MAGIC User Input is being loaded from the **tables.yml** [ https://zg-core-tech-lab.cloud.databricks.com/?o=1374115405812811#files/2748148325491537 ]. 
# MAGIC
# MAGIC User can provide more input tables for comaprison. Each entry adheres to specified criteria below.
# MAGIC - **validation_status**: Only accepts values **True** or **False**. The program will process count operation ony for True validation_status and will skip rows with False or any other invalid_status value. Users can provide False if they choose not to execute the rows again (to optimise execution time by skipping unrequired or already validated tables). Rows labeled with False are excluded from the final output DataFrame.
# MAGIC - **hive_table**: Please provide the Hive table that you want to compare.( Provide with catalog.schema.table_name format )
# MAGIC - **databricks_table**: Please provide corresponding Databricks table of the Hive table for comparison.( Provide with catalog.schema.table_name format )
# MAGIC - **filter_condition**: 
# MAGIC   - Please specify the filter condition using the format commonly used in SQL queries after the WHERE clause. For example: *p_data_date >= date'2024-01-01' AND active_standard IS NOT NULL*. The Filter will be applied to both hive and databricks tables. The output DF will include count values depends on filter condition given. It's recomemended to include filter conditions for partitioned tables to get an accurate counts comparison.
# MAGIC   - Please specify an empty string **""** or **None** if you choose not to add any filter.
# MAGIC - **materialization**: Please provide the materialization used for both tables, e.g., incremental or full_refresh. This column is for informational purposes.
# MAGIC
# MAGIC #### Note:
# MAGIC - Kindly provide the Hive table (production table) and its corresponding Databricks table (stage/test table) mappings with accurate schema for count comparison, as indicated in the provided **tables.yml** list.
# MAGIC - For partitioned tables, it's recommended to add filters.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml

spark = SparkSession.builder.appName("Create DataFrame from Table Data").getOrCreate()

# sample_data
data = yaml.safe_load(open('/Workspace/Users/v-tushara@zillowgroup.com/Mops Data Validation/tables.yml', 'r'))

# Create a list of rows
rows = [(row['validation_status'], row['hive_table'], row['databricks_table'], row['filter_condition'], row['materialization']) for row in data['rows']]

schema = StructType([
    StructField('validation_status', StringType(), nullable=False),
    StructField('hive_table', StringType(), nullable=False),
    StructField('databricks_table', StringType(), nullable=False),
    StructField('filter_condition', StringType(), nullable=False),
    StructField('materialization', StringType(), nullable=False)
])

df = spark.createDataFrame(rows, schema)

def create_count_sql(table, filter):
    if filter == '' or filter == 'null' or filter is None:
        sql = f"SELECT COUNT(*) FROM {table}"
    elif filter and isinstance(filter, str):
        sql = f"SELECT COUNT(*) FROM {table} WHERE {filter}"
    return sql

def collect_partition_cols(hive, databricks):
    hive_meta = f"DESCRIBE EXTENDED {hive}"
    databricks_meta = f"DESCRIBE EXTENDED {databricks}"
    part_info_section_h = False
    part_info_section_d = False
    hive_part_cols = []
    databricks_part_cols= []
    partition_status_h = 'Non-partitioned'
    partition_status_d = 'Non-partitioned'
    try: 
        hive= spark.sql(hive_meta)
        databricks = spark.sql(databricks_meta)
    except Exception as e:
        print(f"Error was encountered while executing the extended DESCRIBE query which is used to collect parition info for the sql.")

    for row in hive.collect():
        if row.col_name and part_info_section_h:
            if row.col_name != '# col_name':
                hive_part_cols.append(row.col_name)
        elif row.col_name == '# Partition Information':
            part_info_section_h = True
            partition_status_h = 'Partitioned'
        elif part_info_section_h:
            part_info_section_h = False
            break
    
    for row in databricks.collect():
        if row.col_name and part_info_section_d:
            if row.col_name != '# col_name':
                databricks_part_cols.append(row.col_name)
        elif row.col_name == '# Partition Information':
            part_info_section_d = True
            partition_status_d = 'Partitioned'
        elif part_info_section_d:
            part_info_section_d = False
            break

    if len(hive_part_cols) != 0 and len(databricks_part_cols)!= 0:
        hive_part_cols = sorted(hive_part_cols)
        databricks_part_cols = sorted(databricks_part_cols)

    return partition_status_h, partition_status_d, hive_part_cols, databricks_part_cols
        

create_count_sql_udf = udf(create_count_sql, StringType())
df = df.withColumn("hive", create_count_sql_udf(df['hive_table'], df['filter_condition'])) \
       .withColumn("databricks", create_count_sql_udf(df['databricks_table'], df['filter_condition']))


collected_counts = []
for row in df.collect():
    hive_sql = row["hive"]
    databricks_sql = row["databricks"]
    validation_status = row["validation_status"].lower()

    if validation_status == 'true':
        try:
            hive_count = spark.sql(hive_sql).collect()[0][0]
            databricks_count = spark.sql(databricks_sql).collect()[0][0]
            filters = row["filter_condition"] if row["filter_condition"] != "" or row["filter_condition"] != 'null' else None 
            part_status_h, part_status_d, hive_part_cols, databricks_part_cols = collect_partition_cols(row["hive_table"], row["databricks_table"])
            collected_counts.append((row["hive_table"], row["databricks_table"], filters, row['materialization'], part_status_h, part_status_d, hive_part_cols, databricks_part_cols, int(hive_count), int(databricks_count)))

        except Exception as e:
            print(f"Error was encountered while executing count SQL queries for row {row}: {str(e)}. This may be due to an issue with the SQL syntax or connectivity problems.")
            print(f"Omitting this row from being included in the final DataFrame")
            continue
    else:
        print(f"Execution process skipped for the row : {row} due to 'false' or invalid value provided for validation_status column by user.")
        continue

schema = StructType([
    StructField("hive_table", StringType(), True),
    StructField("databricks_table", StringType(), True),
    StructField("filters", StringType(), True),
    StructField("materialization", StringType(), True),
    StructField("hive_partition_st", StringType(), True),
    StructField("databricks_partition_st", StringType(), True),
    StructField("hive_part_cols", ArrayType(StringType()), True),
    StructField("databricks_part_cols", ArrayType(StringType()), True),
    StructField("hive_total_cnt", IntegerType(), True),
    StructField("databricks_total_cnt", IntegerType(), True)
])
df = spark.createDataFrame(collected_counts, schema)

df = df.withColumn('total_cnt_difference', df['hive_total_cnt'] - df['databricks_total_cnt']) \
        .withColumn('cnt_match_status', when(col("total_cnt_difference") == 0, True).otherwise(False)) \
        .withColumn('diff_percentage', round((col('total_cnt_difference')/df['hive_total_cnt'])*100, 4))

#Set order columns orders
order = ['hive_table', 'databricks_table', 'filters', 'hive_total_cnt', 'databricks_total_cnt', 'total_cnt_difference', 'diff_percentage','cnt_match_status', 'hive_part_cols', 'databricks_part_cols', 'hive_partition_st', 'databricks_partition_st', 'materialization']
df = df.select(order)

display(df) 
