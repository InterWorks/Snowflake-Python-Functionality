
# Procedure to multiply all members of an input array of integers
# with another input integer

# This particular example would be more useful as a 
# UDF instead of a procedure, however it is useful
# for simply demonstrating the construction of 
# a stored procedure in Snowflake

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the functions for the Stored Procedure

# First define a function which multiplies two integers together
def multiply_together(
    a: int
  , b: int
  ):
  return a*b

# Define main function which maps multiplication function
# to all members of the input array
def multiply_integers_in_array(
    snowpark_session: snowflake.snowpark.Session
  , input_list_py: list
  , input_int_py: int
  ):
  # Use list comprehension to apply the function multiply_together_py
  # to each member of the input list
  return [multiply_together(i, input_int_py) for i in input_list_py]

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import ArrayType
from snowflake.snowpark.types import IntegerType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = multiply_integers_in_array
  , return_type = ArrayType()
  , input_types = [ArrayType(), IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_ALL_INTEGERS_IN_ARRAY'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_MULTIPLY_ALL_INTEGERS_IN_ARRAY([1,2,3,4,5,6,7,8,9], 3)
''').show()
