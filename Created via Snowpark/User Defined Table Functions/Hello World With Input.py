
# Simple UDTF to create a table stating "Hello World"
# over one or two rows, depending on the input

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the class for the UDTF

class simple_table_from_input :
  
  def process(self, row_count: int) :

    # Leverage an if statement to output
    # a different result depending on the input
    if row_count is None or row_count not in [1, 2] :
      return None
    elif row_count == 1 :
      return [(1, 'Hello World')]
    elif row_count == 2 :
      return [
          (1, 'Hello')
        , (2, 'World')
      ]

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import IntegerType, StringType

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = simple_table_from_input
  , output_schema = StructType([StructField("ID", IntegerType()), StructField("NAME", StringType())])
  , input_types = [IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_HELLO_WORLD_WITH_INPUT'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT * FROM TABLE(SNOWPARK_HELLO_WORLD_WITH_INPUT(1))
''').show()

snowpark_session.sql('''
  SELECT * FROM TABLE(SNOWPARK_HELLO_WORLD_WITH_INPUT(2))
''').show()

snowpark_session.sql('''
  SELECT * FROM TABLE(SNOWPARK_HELLO_WORLD_WITH_INPUT(3))
''').show()

