
# Execute a metadata command and drop the results into a table

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def basic_metadata_command_to_table(
    snowpark_session: snowflake.snowpark.Session
  , metadata_command: str
  , destination_table: str
  ):

  ## Read the command into a Snowflake dataframe
  results_df = snowpark_session.sql(metadata_command)

  ## Write the results of the dataframe into a target table
  results_df.write.mode("overwrite").save_as_table(destination_table)
    
  return f"Succeeded: Results inserted into table {destination_table}"

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = basic_metadata_command_to_table
  , return_type = StringType()
  , input_types = [StringType(), StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_BASIC_METADATA_COMMAND_TO_TABLE'
  , replace = True
  , stage_location = '@SPROC_STAGE'
  , execute_as = 'CALLER'
)

### Optionally update stored procedure to execute as CALLER
### if your version of snowflake.snowpark.python does not
### support the execute_as option
snowpark_session.sql('''
  ALTER PROCEDURE IF EXISTS SNOWPARK_BASIC_METADATA_COMMAND_TO_TABLE(VARCHAR, VARCHAR) EXECUTE AS CALLER
''').collect()

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_BASIC_METADATA_COMMAND_TO_TABLE('SHOW DATABASES LIKE \\\'CH%\\\'', 'MY_DESTINATION_TABLE')
''').show()
