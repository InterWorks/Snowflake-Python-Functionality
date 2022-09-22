
# UDF to multiply all members of an input array of integers
# with another input integer

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### First define a function which multiplies two integers together
def multiply_together(
    a: int
  , b: int
  ):
  return a*b

### Define main function which maps multiplication function
### to all members of the input array
def multiply_integers_in_array(
    input_list_py: list
  , input_int_py: int
  ):
  # Use list comprehension to apply the function multiply_together_py
  # to each member of the input list
  return [multiply_together(i, input_int_py) for i in input_list_py]

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import ArrayType
from snowflake.snowpark.types import IntegerType

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = multiply_integers_in_array
  , return_type = ArrayType()
  , input_types = [ArrayType(), IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_MULTIPLY_ALL_INTEGERS_IN_ARRAY'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_MULTIPLY_ALL_INTEGERS_IN_ARRAY([1,2,3,4,5,6,7,8,9], 3)
''').show()

snowpark_session.sql('''
  select 
      array_construct(
          uniform(1, 100, random())::int
        , uniform(1, 100, random())::int
        , uniform(1, 100, random())::int
        , uniform(1, 100, random())::int
        , uniform(1, 100, random())::int
      ) as MY_ARRAY
    , uniform(1, 100, random())::int as MY_INT
    , SNOWPARK_MULTIPLY_ALL_INTEGERS_IN_ARRAY(MY_ARRAY, MY_INT)
  from (table(generator(rowcount => 100)))
''').show()
