
# Simple procedure to multiply an input integer by 3

# This particular example would be more useful as a 
# UDF instead of a procedure, however it is useful
# for simply demonstrating the construction of 
# a stored procedure in Snowflake

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def multiply_by_three(snowpark_session: snowflake.snowpark.Session, input_int_py: int):
  return input_int_py*3

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import IntegerType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = multiply_by_three
  , return_type = IntegerType()
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_INTEGER_BY_THREE'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_MULTIPLY_INTEGER_BY_THREE(20)
''').show()
