
# Simple procedure to executes a chain of SQL commands
# that create and then modify a Snowflake table

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def create_and_modify_table_via_sql_method(snowpark_session: snowflake.snowpark.Session):

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

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = create_and_modify_table_via_sql_method
  , return_type = StringType()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_CREATE_AND_MODIFY_TABLE_VIA_SQL_METHOD'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_CREATE_AND_MODIFY_TABLE_VIA_SQL_METHOD()
''').show()
