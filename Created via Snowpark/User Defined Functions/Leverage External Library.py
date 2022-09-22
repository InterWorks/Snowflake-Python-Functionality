
# UDF to demonstrate leveraging a non-standard library called xlrd

# Details on xlrd can be found here:
# https://xlrd.readthedocs.io/en/latest/

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### Import the required modules 
import xlrd

# Define main function which leverages the mapping
def leverage_external_library(input_int_py: int):

  return xlrd.xldate.xldate_as_datetime(input_int_py, 0)

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
from snowflake.snowpark.types import IntegerType
snowpark_session.add_import('User Defined Functions/Supporting Files/xlrd')

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = leverage_external_library
  , return_type = StringType()
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY(200)
''').show()

snowpark_session.sql('''
  select 
      uniform(1, 100, random())::int as MY_INT
    , SNOWPARK_LEVERAGE_EXTERNAL_LIBRARY(MY_INT)
  from (table(generator(rowcount => 100)))
''').show()
