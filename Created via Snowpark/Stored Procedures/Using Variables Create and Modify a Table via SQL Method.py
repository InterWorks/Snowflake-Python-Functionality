
# Simple procedure to executes a chain of SQL commands
# that create and then modify a Snowflake table,
# using variables for the table name

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def using_variables_create_and_modify_table_via_sql_method(
    snowpark_session : snowflake.snowpark.Session
  , table_name: str
  ):

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

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = using_variables_create_and_modify_table_via_sql_method
  , return_type = StringType()
  , input_types = [StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_USING_VARIABLES_CREATE_AND_MODIFY_TABLE_VIA_SQL_METHOD'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_USING_VARIABLES_CREATE_AND_MODIFY_TABLE_VIA_SQL_METHOD('MY_TEMP_TABLE')
''').show()
