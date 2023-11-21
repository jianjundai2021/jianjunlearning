from datetime import datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pmv_tool.configuration.connections import get_connections
from pmv_tool.connectors.db_connection import DBConnection
import logging


def get_compare_comment(row):
    if row['SOURCE_TABLE_NAME'] == row['TARGET_TABLE_NAME']:
        return 'Eligible to compare. Table exists in both source and target.'
    elif pd.isna(row['SOURCE_TABLE_NAME']) and pd.isna(row['TARGET_TABLE_NAME']):
        return 'Not Eligible to compare. Schema only exists in either source or target.'
    elif pd.notna(row['SOURCE_TABLE_NAME'])  and pd.isna(row['TARGET_TABLE_NAME']):
        return 'Not Eligible to compare. Table only exists in source.'
    elif pd.isna(row['SOURCE_TABLE_NAME']) and pd.notna(row['TARGET_TABLE_NAME']):
        return 'Not Eligible to compare. Table only exists in target.'
    else:
        return 'Unknown.'




# logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

#  to make sure get lint working, need to install pylint and pylint-snowflake

# need get the configuration into dataframe, input-test-object-dataframe
# need get the sql server side of the schema lists from the dataframe
# need get the snowflake side of the schema lists from dataframe
# go to sqlserver information schema, get list of all tables and columns for these schema, sqlserver-column-list
# go to snowflake information schema, get list of all tables and columns for these schema, snowflake-colum-list
# join to the input-test-object-dataframe, sqlserver-column-list,  snowflake-colum-list, full outer join on database_name/schema_name/table_name.
# looping through this dataframe to figuring out the tables indeed need to be summarized, put error message for exceptions
#

# sample of using set operation
# sampleset = {"table1", "table2", "table3", "table4", "table5"}
# sampleset2 = sampleset.copy()
# print(sampleset==sampleset2)

# sampleset2.add("table6")
# print(sampleset==sampleset2)

# alternative approach is:
# 1. Load configuration
# 2. Connect to the source and target database
# 	1. What if we can't connect to one or both?
# 3. For each schema/table comparison set:
# 	1. Check that source and target schemas exist
# 		1. What if one or both don't exist?
# 	2. Retrieve the set of tables we want to test:
# 		1. If there is no list of tables, get all tables from both schemas
# 		2. Compare the two sets of tables:
# 			1. Are the sets identical?
# 				1. What if they're not?
# 					1. Report the discrepancies...
# 		3. For any tables that exist in both source and target:
# 			1. Run comparison tests
# 			2. Report results


# what if the configuration does not exist in configuration file?
# what is the connection does exist but not able to connect?
# connections = get_connections(connection_names=["dev-vm", "qrious-sf"])
connections = get_connections(connection_names=["qrious-sf", "dev-vm"])
print(f'the connections is: {connections}')

connection_sqlserver = connections["dev-vm"]
connection_snowflake = connections["qrious-sf"]

print(f'the connection_snowflake is: {connection_snowflake}')
print(type(connection_snowflake))
print(f'the connection_sqlserver is: {connection_sqlserver}')
print(type(connection_sqlserver))

# We can add check user to the snowflake/sqlserver connector python file
# print("before current user")
# lv_snf_request_user_str = connection_snowflake.run_query("SELECT current_user()")[0][0]
# print(f"Snowflake current user: {lv_snf_request_user_str}")



# build dataframe
# test scenario 1
# input_testing_object_df_x = pd.DataFrame(
#     {
#         'SOURCE_DATABASE_NAME': ["SAMPLE_PRODUCTION_DB", "SAMPLE_PRODUCTION_DB", "SAMPLE_PRODUCTION_DB", "SAMPLE_PRODUCTION_DB", "SAMPLE_PRODUCTION_DB" ],
#         'SOURCE_SCHEMA_NAME': ["SCH_SQLSERVER_X", "SCH_SQLSERVER_Y", "SCH_SQLSERVER_Z", "SCH_SQLSERVER_A", "SCH_SQLSERVER_B"],
#         'SOURCE_TABLE_NAME': ['ALL TABLES', 'ALL TABLES', 'ALL TABLES', 'ALL TABLES', 'ALL TABLES'],
#         'TARGET_DATABASE_NAME': ["PMV_SAMPLE", "PMV_SAMPLE", "PMV_SAMPLE", "PMV_SAMPLE", "PMV_SAMPLE" ],
#         'TARGET_SCHEMA_NAME': ["SCH_SNOWFLAKE_X", "SCH_SNOWFLAKE_Y", "SCH_SNOWFLAKE_Z", "SCH_SNOWFLAKE_A", "SCH_SNOWFLAKE_B"]  ,
#         'TARGET_TABLE_NAME': ['ALL TABLES', 'ALL TABLES', 'ALL TABLES', 'ALL TABLES', 'ALL TABLES'],
#     }
# )

# test scenario 2
input_testing_object_df = pd.DataFrame(
    {
        'SOURCE_DATABASE_NAME': ['SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB', 'SAMPLE_PRODUCTION_DB'],
        'SOURCE_SCHEMA_NAME': ['SCH_SQLSERVER_X', 'SCH_SQLSERVER_X', 'SCH_SQLSERVER_X', 'SCH_SQLSERVER_Y', 'SCH_SQLSERVER_Y', 'SCH_SQLSERVER_Z', 'SCH_SQLSERVER_Z', 'SCH_SQLSERVER_A', 'SCH_SQLSERVER_A', 'SCH_SQLSERVER_B', 'SCH_SQLSERVER_B'],
        'SOURCE_TABLE_NAME': ['TABLE_XA', 'TABLE_XB', 'TABLE_XZ', 'TABLE_YA', 'TABLE_YD', 'TABLE_ZC', 'TABLE_ZD', 'TABLE_AA', 'TABLE_AB', 'TABLE_BA', 'TABLE_BB'],
        'TARGET_DATABASE_NAME': ['PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE', 'PMV_SAMPLE'],
        'TARGET_SCHEMA_NAME': ['SCH_SNOWFLAKE_X', 'SCH_SNOWFLAKE_X', 'SCH_SNOWFLAKE_X', 'SCH_SNOWFLAKE_Y', 'SCH_SNOWFLAKE_Y', 'SCH_SNOWFLAKE_Z', 'SCH_SNOWFLAKE_Z', 'SCH_SNOWFLAKE_A', 'SCH_SNOWFLAKE_A', 'SCH_SNOWFLAKE_B', 'SCH_SNOWFLAKE_B'],
        'TARGET_TABLE_NAME': ['TABLE_XA', 'TABLE_XB', 'TABLE_XZ', 'TABLE_YA', 'TABLE_YD', 'TABLE_ZC', 'TABLE_ZD', 'TABLE_AA', 'TABLE_AB', 'TABLE_BA', 'TABLE_BB']
    }
)

# convert any lower case configuration data to upper case
for column in input_testing_object_df.columns:
    input_testing_object_df[column] = input_testing_object_df[column].str.upper()

# add the full name for source and target, ie database name + schema name + table name
input_testing_object_df['SOURCE_OBJECT_FULL_NAME'] = '\'' + input_testing_object_df['SOURCE_DATABASE_NAME'] + '.' + input_testing_object_df['SOURCE_SCHEMA_NAME'] + '.' + input_testing_object_df['SOURCE_TABLE_NAME'] + '\''
input_testing_object_df['TARGET_OBJECT_FULL_NAME'] = '\'' + input_testing_object_df['TARGET_DATABASE_NAME'] + '.' + input_testing_object_df['TARGET_SCHEMA_NAME'] + '.' + input_testing_object_df['TARGET_TABLE_NAME'] + '\''

print (f'the input_testing_object_df is: \n {input_testing_object_df}')

# categorize the input_testing_object_df into two dataframes, one for schema level, one for table level
input_testing_schema_df = input_testing_object_df[input_testing_object_df["SOURCE_TABLE_NAME"]=='ALL TABLES'].drop(columns=["SOURCE_TABLE_NAME", "TARGET_TABLE_NAME"])
input_testing_table_df = input_testing_object_df[input_testing_object_df["SOURCE_TABLE_NAME"]!='ALL TABLES']

# process the schema level dataframe for sql server
## get the sql server side of the metadata for schema level
sqlserver_database_name = input_testing_schema_df["SOURCE_DATABASE_NAME"].unique().tolist()[0]
print(f'the sqlserver_database_name is: {sqlserver_database_name}')
sqlserver_schema_list = ('\'' + input_testing_schema_df["SOURCE_SCHEMA_NAME"] + '\'').unique().tolist()
print(f'the sqlserver_schema_list is: {sqlserver_schema_list}')

sqlserver_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where tbl.table_catalog = '{sqlserver_database_name}'
and tbl.table_schema in ({', '.join(sqlserver_schema_list)})
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""

print(f'the sqlserver_table_list_str is: {sqlserver_table_list_str}')

sqlserver_table_list_df = connection_sqlserver.read_as_df(sqlserver_table_list_str)
print(f'the sqlserver_table_list_df is: \n {sqlserver_table_list_df}')

## get the snowflake side of the metadata fo schema level

snowflake_database_name = input_testing_object_df["TARGET_DATABASE_NAME"].unique().tolist()[0]
snowflake_schema_list = ('\''+ input_testing_object_df["TARGET_SCHEMA_NAME"] + '\'').unique().tolist()
snowflake_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where upper(tbl.table_catalog) = upper('{snowflake_database_name}')
and upper(tbl.table_schema) in ({', '.join(snowflake_schema_list)})
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""

snowflake_table_list_df = connection_snowflake.read_as_df(snowflake_table_list_str)

print(f'the snowflake_table_list_str is: {snowflake_table_list_str}')
print(f'the snowflake_table_list_df is: \n {snowflake_table_list_df}')


testing_tables_joined_sqlserver = pd.merge(input_testing_schema_df, sqlserver_table_list_df, how='left', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "SOURCE_TABLE_NAME"})
print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")

testing_tables_joined_snowflake = pd.merge(input_testing_schema_df, snowflake_table_list_df, how='left', left_on=['TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "TARGET_TABLE_NAME"})
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

output_schema_level_df = pd.merge(testing_tables_joined_sqlserver, testing_tables_joined_snowflake, how='outer', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'SOURCE_TABLE_NAME'], right_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'TARGET_TABLE_NAME'])
print(f"testing_tables_joined_all is: \n {output_schema_level_df}")

# Apply the custom function
output_schema_level_df['RESULT'] = output_schema_level_df.apply(get_compare_comment, axis=1)
print(f"output_schema_level_df is: \n {output_schema_level_df}")




# process the table level dataframe for sql server
## get the sql server side of the metadata for schema level
sqlserver_database_name = input_testing_table_df["SOURCE_DATABASE_NAME"].unique().tolist()[0]
print(f'the sqlserver_database_name is: {sqlserver_database_name}')

sqlserver_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where upper(tbl.table_catalog) = upper('{sqlserver_database_name}')
and  upper(tbl.table_catalog) + '.' + upper(tbl.table_schema) + '.' + upper(tbl.table_name)  in ( {', '.join(input_testing_object_df['SOURCE_OBJECT_FULL_NAME'].tolist())} )
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""

print(f'the sqlserver_table_list_str is: {sqlserver_table_list_str}')

sqlserver_table_list_df_tbl = connection_sqlserver.read_as_df(sqlserver_table_list_str)
print(f'the sqlserver_table_list_df is: \n {sqlserver_table_list_df_tbl}')

snowflake_database_name = input_testing_table_df["TARGET_DATABASE_NAME"].unique().tolist()[0]
snowflake_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where upper(tbl.table_catalog) = upper('{snowflake_database_name}')
and  upper(tbl.table_catalog) || '.' || upper(tbl.table_schema) || '.' || upper(tbl.table_name)  in ( {', '.join(input_testing_object_df['SOURCE_OBJECT_FULL_NAME'].tolist())} )
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""
snowflake_table_list_df = connection_snowflake.read_as_df(snowflake_table_list_str)

testing_tables_joined_sqlserver = pd.merge(input_testing_schema_df, sqlserver_table_list_df, how='left', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "SOURCE_TABLE_NAME"})
print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")

testing_tables_joined_snowflake = pd.merge(input_testing_schema_df, snowflake_table_list_df, how='left', left_on=['TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "TARGET_TABLE_NAME"})
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

output_table_level_df = pd.merge(testing_tables_joined_sqlserver, testing_tables_joined_snowflake, how='outer', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'SOURCE_TABLE_NAME'], right_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'TARGET_TABLE_NAME'])
print(f"testing_tables_joined_all is: \n {output_schema_level_df}")

# Apply the custom function
output_table_level_df['RESULT'] = output_table_level_df.apply(get_compare_comment, axis=1)
print(f"output_table_level_df is: \n {output_table_level_df}")


exit()



sqlserver_database_name = input_testing_object_df["SOURCE_DATABASE_NAME"].unique().tolist()[0]
snowflake_database_name = input_testing_object_df["TARGET_DATABASE_NAME"].unique().tolist()[0]
print(f'the snowflake_database_name is: {snowflake_database_name}')

sqlserver_schema_list = input_testing_object_df["SOURCE_SCHEMA_NAME"].unique().tolist()
print(f'the sqlserver_schema_list is: {sqlserver_schema_list}')

snowflake_schema_list = input_testing_object_df["TARGET_SCHEMA_NAME"].unique().tolist()
print(f'the snowflake_schema_list is: {snowflake_schema_list}')

sqlserver_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where tbl.table_catalog = '{sqlserver_database_name}'
-- and (tbl.table_schema in ({', '.join([f'"{schema}"' for schema in sqlserver_schema_list])})
and  upper(tbl.table_catalog) + '.' + upper(tbl.table_schema) + '.' + upper(tbl.table_name)  in ( {', '.join(input_testing_object_df['SOURCE_OBJECT_FULL_NAME'].tolist())} )
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""

print(f'the sqlserver_table_list_str is: {sqlserver_table_list_str}')

sqlserver_table_list_df = connection_sqlserver.read_as_df(sqlserver_table_list_str)
print(f'the sqlserver_table_list_df is: \n {sqlserver_table_list_df}')

exit()

snowflake_table_list_str =f"""
select
    tbl.table_catalog as TABLE_CATALOG,
    tbl.table_schema as TABLE_SCHEMA,
    tbl.table_name as TABLE_NAME
from information_schema.tables tbl
where tbl.table_catalog = '{snowflake_database_name}'
and tbl.table_schema in ({', '.join([f'"{schema}"' for schema in snowflake_schema_list])})
order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""

snowflake_table_list_df = connection_snowflake.read_as_df(snowflake_table_list_str)

print(f'the snowflake_table_list_str is: {snowflake_table_list_str}')
print(f'the snowflake_table_list_df is: \n {snowflake_table_list_df}')


print(f"input_testing_object_df is: \n {input_testing_object_df}")


testing_schemas_df = input_testing_object_df[input_testing_object_df["SOURCE_TABLE_NAME"]=='ALL TABLES'].drop(columns=["SOURCE_TABLE_NAME", "TARGET_TABLE_NAME"])

testing_schematable_df = input_testing_object_df[input_testing_object_df["SOURCE_TABLE_NAME"]!='ALL TABLES']
# .drop(columns=["SOURCE_TABLE_NAME", "TARGET_TABLE_NAME"])
print(f"testing_schematable_df is: \n {testing_schematable_df}")


exit()



print(f"testing_schemas_df is: \n {testing_schemas_df}")
print(f"sqlserver_table_list_df is: \n {sqlserver_table_list_df}")

testing_tables_joined_sqlserver = pd.merge(testing_schemas_df, sqlserver_table_list_df, how='left', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "SOURCE_TABLE_NAME"})
print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")

# testing_tables_joined_sqlserver_filtered = testing_tables_joined_sqlserver[testing_tables_joined_sqlserver["TABLE_NAME"].notnull()]
# print(f"testing_tables_joined_sqlserver_filtered is: \n {testing_tables_joined_sqlserver_filtered}")

print(f"testing_schemas_df is: \n {testing_schemas_df}")
print(f"snowflake_table_list_df is: \n {snowflake_table_list_df}")

testing_tables_joined_snowflake = pd.merge(testing_schemas_df, snowflake_table_list_df, how='left', left_on=['TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME'], right_on=['TABLE_CATALOG', 'TABLE_SCHEMA']).drop("TABLE_CATALOG", axis=1).drop("TABLE_SCHEMA", axis=1).rename(columns={"TABLE_NAME": "TARGET_TABLE_NAME"})
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

# testing_tables_joined_snowflake_filtered = testing_tables_joined_snowflake[testing_tables_joined_snowflake["TABLE_NAME"].notnull()]
# print(f"testing_tables_joined_snowflake_filtered is: \n {testing_tables_joined_snowflake_filtered}")

print(f"testing_tables_joined_sqlserver is: \n {testing_tables_joined_sqlserver}")
print(f"testing_tables_joined_snowflake is: \n {testing_tables_joined_snowflake}")

testing_tables_joined_all = pd.merge(testing_tables_joined_sqlserver, testing_tables_joined_snowflake, how='outer', left_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'SOURCE_TABLE_NAME'], right_on=['SOURCE_DATABASE_NAME', 'SOURCE_SCHEMA_NAME', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME', 'TARGET_TABLE_NAME'])
print(f"testing_tables_joined_all is: \n {testing_tables_joined_all}")



# Apply the custom function
testing_tables_joined_all['RESULT'] = testing_tables_joined_all.apply(get_compare_comment, axis=1)
print(f"testing_tables_joined_all is: \n {testing_tables_joined_all}")
