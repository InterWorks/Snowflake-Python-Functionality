
-- Simple procedure to executes a chain of SQL commands
-- that create and then modify a Snowflake table,
-- using variables for the table name

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE using_variables_create_and_modify_table_via_sql_method(INPUT_TABLE_NAME STRING)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'using_variables_create_and_modify_table_via_sql_method_py'
as
$$
def using_variables_create_and_modify_table_via_sql_method_py(snowpark_session, table_name: str):

  # This procedure uses standard python 
  # string manipulation to leverage variables
  # within strings. For example, we can
  # insert table_name into a string as follows:
  example_string = f'My table name is {table_name}'

  ## Execute the query to create the table
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql(f'''
    CREATE OR REPLACE TABLE {table_name} (
        RECORD_ID INT IDENTITY
      , USER_NAME STRING
    ) 
  ''').collect()
  
  ## Execute the query to drop the RECORD_ID field
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql(f'''
    ALTER TABLE {table_name}
    DROP COLUMN RECORD_ID
  ''').collect()
  
  ## Execute the query to add the TIMESTAMP field
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql(f'''
    ALTER TABLE {table_name}
    ADD COLUMN TIMESTAMP STRING
  ''').collect()
  
  ## Execute the query to insert a new record
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql(f'''
    INSERT INTO {table_name} (USER_NAME, TIMESTAMP)
    SELECT CURRENT_USER, CURRENT_TIMESTAMP
  ''').collect()

  ## Execute a star select query into a Snowflake dataframe
  results = snowpark_session.sql(f'SELECT * FROM {table_name}').collect()

  ## Execute the query to drop the table again
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql(f'''
    DROP TABLE IF EXISTS {table_name}
  ''').collect()

  return results
$$
;

------------------------------------------------------------------
-- Testing

call using_variables_create_and_modify_table_via_sql_method('MY_TEMP_TABLE');

