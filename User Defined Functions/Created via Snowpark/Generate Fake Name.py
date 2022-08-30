
# UDF to generate a fake name using faker

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### Import the required modules 
from faker import Faker

### Define main function which generates a fake name
def generate_fake_name():
  fake = Faker()
  return fake.name()

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('faker')

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = generate_fake_name
  , return_type = StringType()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_FAKE_NAME'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_GENERATE_FAKE_NAME()
''').show()

snowpark_session.sql('''
  select 
      SNOWPARK_GENERATE_FAKE_NAME()
  from (table(generator(rowcount => 100)))
''').show()
