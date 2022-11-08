
-- UDTF to leverage an xlsx mapping file from a stage

------------------------------------------------------------------
-- Create the stage and table to store the demo data

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS;

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This is empty to start, so we
-- need to upload our "Dummy Mapping File.xlsx"
-- file into this stage. We do not
-- compress the file so that we can keep
-- the example function simple. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Dummy Mapping File.xlsx' @STG_FILES_FOR_UDTFS AUTO_COMPRESS = FALSE;
*/

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_UDTFS/Dummy Mapping File.xlsx'; 

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION LEVERAGE_EXTERNAL_MAPPING_FILE (
      INPUT_ITEM STRING
  )
  returns TABLE (
      MAPPED_ITEM STRING
    )
  language python
  runtime_version = '3.8'
  packages = ('pandas', 'openpyxl') -- openpyxl required for pandas to read xlsx
  imports = ('@STG_FILES_FOR_UDTFS/Dummy Mapping File.xlsx')
  handler = 'leverage_external_mapping_file'
as
$$

# Import the required modules 
import pandas
import sys

# Retrieve the Snowflake import directory
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

# Ingest the mapping into a Pandas dataframe
df_mapping = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")

# Define handler class
class leverage_external_mapping_file :
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_item: str
    ) :

    ### Apply the mapping to retrieve the mapped value
    df_mapped_group = df_mapping[df_mapping['Item']==input_item]
  
    mapped_group = 'No matching group found'
    
    if len(df_mapped_group.index) > 0 :
      mapped_group = df_mapped_group.iloc[0]['Group']

    yield(mapped_group,)
  
$$
;
 
------------------------------------------------------------------
-- Testing

with test_values as (
  select $1::string as ITEM
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
    *
from test_values
  , table(LEVERAGE_EXTERNAL_MAPPING_FILE(ITEM))
;
