
-- UDF to leverage an xlsx mapping file from a stage

------------------------------------------------------------------
-- Create the stage

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDFS;

-- View files in stage
LIST @STG_FILES_FOR_UDFS;

-- This is empty to start, so we
-- need to upload our "Dummy Mapping File.xlsx"
-- file into this stage. We do not
-- compress the file so that we can keep
-- the example function simple. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Dummy Mapping File.xlsx' @STG_FILES_FOR_UDFS AUTO_COMPRESS = FALSE ;
*/

-- View files in stage
LIST @STG_FILES_FOR_UDFS; 

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_UDFS/Dummy Mapping File.xlsx'; 

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION leverage_external_mapping_file(INPUT_ITEM string)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('pandas', 'openpyxl') -- openpyxl required for pandas to read xlsx
  imports = ('@STG_FILES_FOR_UDFS/Dummy Mapping File.xlsx')
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
def leverage_external_mapping_file_py(input_item_py: str):

  df_mapping = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")

  df_mapped_group = df_mapping[df_mapping['Item']==input_item_py]
  
  mapped_group = 'No matching group found'
  
  if len(df_mapped_group.index) > 0 :
    mapped_group = df_mapped_group.iloc[0]['Group']

  return mapped_group
$$
;

------------------------------------------------------------------
-- Testing

select leverage_external_mapping_file('Chocolate');
select leverage_external_mapping_file('Milk');
select leverage_external_mapping_file('Bread');

with test_values as (
  select $1::string as item
  from values 
      ('Chocolate')
    , ('Coffee')
    , ('Banana')
    , ('Milk')
    , ('Orange Juice')
    , ('Orange')
    , ('Bread')
    , ('Cheese')
)
select 
    item
  , leverage_external_mapping_file(item) as item_group
from test_values
;
