
-- Simple procedure to executes a chain of SQL commands
-- that create and then modify a Snowflake table

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE create_and_modify_table_via_sql_method()
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'create_and_modify_table_via_sql_method_py'
as
$$
def create_and_modify_table_via_sql_method_py(snowpark_session):

  ## Execute the query to create the table
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql('''
    CREATE OR REPLACE TABLE MY_TEMP_TABLE (
        RECORD_ID INT IDENTITY
      , USER_NAME STRING
    ) 
  ''').collect()
  
  ## Execute the query to drop the RECORD_ID field
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql('''
    ALTER TABLE MY_TEMP_TABLE
    DROP COLUMN RECORD_ID
  ''').collect()
  
  ## Execute the query to add the TIMESTAMP field
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql('''
    ALTER TABLE MY_TEMP_TABLE
    ADD COLUMN TIMESTAMP STRING
  ''').collect()
  
  ## Execute the query to insert a new record
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql('''
    INSERT INTO MY_TEMP_TABLE (USER_NAME, TIMESTAMP)
    SELECT CURRENT_USER, CURRENT_TIMESTAMP
  ''').collect()

  ## Execute a star select query into a Snowflake dataframe
  results = snowpark_session.sql('SELECT * FROM MY_TEMP_TABLE').collect()

  ## Execute the query to drop the table again
  ## using ''' for a multi-line string input
  ## and .collect() to ensure execution on Snowflake
  snowpark_session.sql('''
    DROP TABLE IF EXISTS MY_TEMP_TABLE
  ''').collect()

  return results
$$
;

------------------------------------------------------------------
-- Testing

call create_and_modify_table_via_sql_method();

