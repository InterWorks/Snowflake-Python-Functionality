
# Simple UDF to multiply an input integer by 3

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

def multiply_by_three(input_int_py: int):
  return input_int_py*3

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import IntegerType

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = multiply_by_three
  , return_type = IntegerType()
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_INTEGER_BY_THREE'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_MULTIPLY_INTEGER_BY_THREE(20)
''').show()

snowpark_session.sql('''
  select 
      uniform(1, 100, random())::int as MY_INT
    , SNOWPARK_MULTIPLY_INTEGER_BY_THREE(MY_INT)
  from (table(generator(rowcount => 100)))
''').show()
