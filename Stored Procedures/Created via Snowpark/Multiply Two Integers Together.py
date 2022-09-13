
# Simple procedure to multiply two input integers together

# This particular example would be more useful as a 
# UDF instead of a procedure, however it is useful
# for simply demonstrating the construction of 
# a stored procedure in Snowflake

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def multiply_together(
    snowpark_session: snowflake.snowpark.Session
  , input_int_py_1: int
  , input_int_py_2: int
  ):
  return input_int_py_1*input_int_py_2

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import IntegerType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = multiply_together
  , return_type = IntegerType()
  , input_types = [IntegerType(), IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_TWO_INTEGERS_TOGETHER'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_MULTIPLY_TWO_INTEGERS_TOGETHER(20, 7)
''').show()
