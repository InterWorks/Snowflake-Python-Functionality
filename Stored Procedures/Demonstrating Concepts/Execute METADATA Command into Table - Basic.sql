
-- Execute a metadata command and drop the results into a table

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE basic_metadata_command_to_table(
      INPUT_METADATA_COMMAND STRING
    , INPUT_DESTINATION_TABLE STRING
  )
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'basic_metadata_command_to_table_py'
  execute as caller
as
$$
def basic_metadata_command_to_table_py(snowpark_session, metadata_command: str, destination_table: str):

  ## Read the command into a Snowflake dataframe
  results_df = snowpark_session.sql(metadata_command)

  ## Write the results of the dataframe into a target table
  results_df.write.mode("overwrite").save_as_table(destination_table)
    
  return f"Succeeded: Results inserted into table {destination_table}"
$$
;

------------------------------------------------------------------
-- Testing

call basic_metadata_command_to_table('SHOW DATABASES LIKE \'CH%\'', 'MY_DESTINATION_TABLE');

