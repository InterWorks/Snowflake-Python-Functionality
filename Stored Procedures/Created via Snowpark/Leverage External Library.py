
# Stored procedure to demonstrate leveraging a non-standard library called xlrd

# Details on xlrd can be found here:
# https://xlrd.readthedocs.io/en/latest/

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

# Import the required modules 
import xlrd

# Define main function which leverages the package
def leverage_external_library(
    snowpark_session: snowflake.snowpark.Session
  , input_int_py: int
  ):

  return xlrd.xldate.xldate_as_datetime(input_int_py, 0)

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
from snowflake.snowpark.types import IntegerType
snowpark_session.add_packages('snowflake-snowpark-python')
snowpark_session.add_import('Stored Procedures/Supporting Files/xlrd')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = leverage_external_library
  , return_type = StringType()
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY(200)
''').show()
