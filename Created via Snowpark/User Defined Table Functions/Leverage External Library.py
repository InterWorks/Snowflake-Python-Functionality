
# UDTF to demonstrate leveraging a non-standard library called xlrd

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
need to upload our xlrd
directory into this stage. We do not
compress the file so that we can keep
the example function simple. This is achieved
with the following commands in a local
SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/xlrd/xldate.py' @STG_FILES_FOR_UDTFS/xlrd AUTO_COMPRESS = FALSE ;
*/
'''

# This file deliberately has spaces in the name
# so that we can also demonstrate how to handle that

# View specific file in stage
snowpark_session.sql('''
  LIST '@STG_FILES_FOR_UDTFS/xlrd/'; 
''').show()

##################################################################
## Define the class for the UDTF

# Import the required modules 
import sys

# Spoof sys._xoptions so that class can be created
sys._xoptions["snowflake_import_directory"] = 'Supporting Files/xlrd/'

# Define handler class
class leverage_external_library :
  
  ## Retrieve the Snowflake import directory
  IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
  import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

  ## Import the required external modules using the importlib.util library
  import importlib.util
  module_spec = importlib.util.spec_from_file_location('xldate', import_dir + 'xldate.py')
  xldate = importlib.util.module_from_spec(module_spec)
  module_spec.loader.exec_module(xldate)
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_int: int
    ) :

    ### Apply the mapping to retrieve the mapped value
    xl_datetime = self.xldate.xldate_as_datetime(input_int, 0)

    yield(xl_datetime,)

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import TimestampType, IntegerType
snowpark_session.add_packages('pandas', 'openpyxl') # openpyxl required for pandas to read xlsx
snowpark_session.add_import('Supporting Files/xlrd')

### Define output schema
output_schema = StructType([
      StructField("EXCEL_TIMESTAMP", TimestampType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = leverage_external_library
  , output_schema = output_schema
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  with test_values as (
    select
        uniform(1, 100, random())::int as MY_INT
    from (table(generator(rowcount => 100)))
  )
  select
      *
  from test_values
    , table(SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY(MY_INT))
''').show()
