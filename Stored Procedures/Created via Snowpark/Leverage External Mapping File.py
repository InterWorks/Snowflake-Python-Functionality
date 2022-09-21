
# Stored procedure to leverage an xlsx mapping file from a stage

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

# Import the required modules 
import pandas
import sys

# Define main function which leverages the mapping
def leverage_external_mapping_file(
    snowpark_session: snowflake.snowpark.Session
  , destination_table: str
  ):

  # Retrieve the Snowflake import directory
  IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
  import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

  # Read mapping table using Pandas
  mapping_df_pd = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")

  # Convert the filtered Pandas dataframe into a Snowflake dataframe
  mapping_df_sf = snowpark_session.create_dataframe(mapping_df_pd)
  
  # Write the results of the dataframe into a target table
  mapping_df_sf.write.mode("overwrite").save_as_table(destination_table)
    
  return f"Succeeded: Results inserted into table {destination_table}"

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python', 'pandas', 'openpyxl')
snowpark_session.add_import('Stored Procedures/Supporting Files/Dummy Mapping File.xlsx')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = leverage_external_mapping_file
  , return_type = StringType()
  , input_types = [StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE('IMPORTED_MAPPING_TABLE')
''').show()
