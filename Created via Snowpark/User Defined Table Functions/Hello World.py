
# Simple UDTF to create a table stating "Hello World" over two rows

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the class for the UDTF

class simple_table :
  
  def process(self) :
    '''
    Enter Python code here that 
    executes for each input row.
    This ends with a set of yield
    clauses that output tuples
    '''
    yield (1, 'Hello')
    yield (2, 'World')
    
    '''
    Alternatively, this may end with
    a single return clause containing
    an iterable of tuples, for example:
    
    return [
        (1, 'Hello')
      , (2, 'World')
    ]
    '''

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import IntegerType, StringType

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = simple_table
  , output_schema = StructType([StructField("ID", IntegerType()), StructField("NAME", StringType())])
  , is_permanent = True
  , name = 'SNOWPARK_HELLO_WORLD'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT * FROM TABLE(SNOWPARK_HELLO_WORLD())
''').show()

