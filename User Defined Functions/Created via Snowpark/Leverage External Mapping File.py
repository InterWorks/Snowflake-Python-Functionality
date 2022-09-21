
# UDF to leverage a local xlsx mapping file

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### Import the required modules 
import pandas
import openpyxl # openpyxl required for pandas to read xlsx
import sys

### Define main function which leverages the mapping
def leverage_external_mapping_file(input_item_py: str):

  # Retrieve the Snowflake import directory
  IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
  import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

  # Read mapping into Pandas
  df_mapping = pandas.read_excel(import_dir + 'Dummy Mapping File.xlsx', skiprows=5, usecols="C:D")

  # Map input value
  df_mapped_group = df_mapping[df_mapping['Item']==input_item_py]
  mapped_group = 'No matching group found'
  
  if len(df_mapped_group.index) > 0 :
    mapped_group = df_mapped_group.iloc[0]['Group']

  return mapped_group

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages(['pandas', 'openpyxl'])
snowpark_session.add_import('User Defined Functions/Supporting Files/Dummy Mapping File.xlsx')

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = leverage_external_mapping_file
  , return_type = StringType()
  , input_types = [StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE('Chocolate')
''').show()

snowpark_session.sql('''
  SELECT SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE('Milk')
''').show()

snowpark_session.sql('''
  SELECT SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE('Bread')
''').show()

snowpark_session.sql('''
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
    , SNOWPARK_LEVERAGE_EXTERNAL_MAPPING_FILE(item) as item_group
  from test_values
''').show()
