
# UDTF to leverage an xlsx mapping file from a stage

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Create the stage and table to store the demo data

# Create the stage if it does not exist
snowpark_session.sql('''
  CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS
''').collect()

'''
This is empty to start, so we
need to upload our "Dummy Mapping File.xlsx"
file into this stage. We do not
compress the file so that we can keep
the example function simple. This is achieved
with the following commands in a local
SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Dummy Mapping File.xlsx' @STG_FILES_FOR_UDTFS AUTO_COMPRESS = FALSE;
*/
'''

# This file deliberately has spaces in the name
# so that we can also demonstrate how to handle that

# View specific file in stage
snowpark_session.sql('''
  LIST '@STG_FILES_FOR_UDTFS/Dummy Mapping File.xlsx';
''').show()

##################################################################
## Define the class for the UDTF

# Import the required modules 
import pandas
import sys

# Spoof sys._xoptions so that class can be created
sys._xoptions["snowflake_import_directory"] = 'Supporting Files/'

# Define handler class
class leverage_external_mapping_file :

  ## Retrieve the Snowflake import directory
  IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
  import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

  ## Ingest the mapping into a Pandas dataframe
  df_mapping = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_item: str
    ) :

    ### Apply the mapping to retrieve the mapped value
    df_mapped_group = self.df_mapping[self.df_mapping['Item']==input_item]
  
    mapped_group = 'No matching group found'
    
    if len(df_mapped_group.index) > 0 :
      mapped_group = df_mapped_group.iloc[0]['Group']

    yield(mapped_group,)

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('pandas', 'openpyxl') # openpyxl required for pandas to read xlsx
snowpark_session.add_import('Supporting Files/Dummy Mapping File.xlsx')

### Define output schema
output_schema = StructType([
      StructField("MAPPED_ITEM", StringType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = leverage_external_mapping_file
  , output_schema = output_schema
  , input_types = [StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
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
    , table(SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE(ITEM))
''').show()
