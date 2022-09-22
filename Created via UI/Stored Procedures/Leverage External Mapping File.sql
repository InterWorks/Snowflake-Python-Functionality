
-- Stored procedure to leverage an xlsx mapping file from a stage

------------------------------------------------------------------
-- Create the stage

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_STORED_PROCEDURES;

-- View files in stage
LIST @STG_FILES_FOR_STORED_PROCEDURES;

-- This is empty to start, so we
-- need to upload our "Dummy Mapping File.xlsx"
-- file into this stage. We do not
-- compress the file so that we can keep
-- the example function simple. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Dummy Mapping File.xlsx' @STG_FILES_FOR_STORED_PROCEDURES AUTO_COMPRESS = FALSE ;
*/

-- View files in stage
LIST @STG_FILES_FOR_STORED_PROCEDURES; 

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_STORED_PROCEDURES/Dummy Mapping File.xlsx'; 

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE leverage_external_mapping_file(INPUT_DESTINATION_TABLE STRING)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'pandas', 'openpyxl') -- openpyxl required for pandas to read xlsx
  imports = ('@STG_FILES_FOR_STORED_PROCEDURES/Dummy Mapping File.xlsx')
  handler = 'leverage_external_mapping_file_py'
as
$$

# Import the required modules 
import pandas
import sys

# Retrieve the Snowflake import directory
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

# Define main function which leverages the mapping
def leverage_external_mapping_file_py(snowpark_session, destination_table: str):

  # Read mapping table using Pandas
  mapping_df_pd = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")

  # Convert the filtered Pandas dataframe into a Snowflake dataframe
  mapping_df_sf = snowpark_session.create_dataframe(mapping_df_pd)
  
  # Write the results of the dataframe into a target table
  mapping_df_sf.write.mode("overwrite").save_as_table(destination_table)
    
  return f"Succeeded: Results inserted into table {destination_table}"

$$
;

------------------------------------------------------------------
-- Testing

call leverage_external_mapping_file('IMPORTED_MAPPING_TABLE');

select * from IMPORTED_MAPPING_TABLE;