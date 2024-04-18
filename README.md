# Tables-Count-Data-Validation-Script
Tables Count Data Validation Script

#### User Inputs:
User Input is being loaded from the **tables.yml**

User can provide more input tables for comaprison. Each entry adheres to specified criteria below.
- **validation_status**: Only accepts values **True** or **False**. The program will process count operation ony for True validation_status and will skip rows with False or any other invalid_status value. Users can provide False if they choose not to execute the rows again (to optimise execution time by skipping unrequired or already validated tables). Rows labeled with False are excluded from the final output DataFrame.
- **hive_table**: Please provide the Hive table that you want to compare.( Provide with catalog.schema.table_name format )
- **databricks_table**: Please provide corresponding Databricks table of the Hive table for comparison.( Provide with catalog.schema.table_name format )
- **filter_condition**: 
  - Please specify the filter condition using the format commonly used in SQL queries after the WHERE clause. For example: *p_data_date >= date'2024-01-01' AND active_standard IS NOT NULL*. The Filter will be applied to both hive and databricks tables. The output DF will include count values depends on filter condition given. It's recomemended to include filter conditions for partitioned tables to get an accurate counts comparison.
  - Please specify an empty string **""** or **None** if you choose not to add any filter.
- **materialization**: Please provide the materialization used for both tables, e.g., incremental or full_refresh. This column is for informational purposes.

#### Note:
- Kindly provide the Hive table (production table) and its corresponding Databricks table (stage/test table) mappings with accurate schema for count comparison, as indicated in the provided **tables.yml** list.
- For partitioned tables, it's recommended to add filters.
