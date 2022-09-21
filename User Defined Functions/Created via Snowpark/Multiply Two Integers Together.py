
# Simple UDF to multiply two input integers together

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

def multiply_together(
    input_int_py_1: int
  , input_int_py_2: int
  ):
  return input_int_py_1*input_int_py_2

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import IntegerType

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = multiply_together
  , return_type = IntegerType()
  , input_types = [IntegerType(), IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_TWO_INTEGERS_TOGETHER'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_MULTIPLY_TWO_INTEGERS_TOGETHER(20, 7)
''').show()

snowpark_session.sql('''
  select 
      uniform(1, 100, random())::int as MY_INT_1
    , uniform(1, 100, random())::int as MY_INT_2
    , SNOWPARK_MULTIPLY_TWO_INTEGERS_TOGETHER(MY_INT_1, MY_INT_2)
  from (table(generator(rowcount => 100)))
''').show()
