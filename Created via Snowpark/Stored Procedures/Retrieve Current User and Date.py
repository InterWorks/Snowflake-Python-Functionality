
# Simple procedure to execute a SQL command that
# contains the current user and date

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

def retrieve_current_user_and_date(snowpark_session: snowflake.snowpark.Session):

  ## Execute the query into a Snowflake dataframe
  results_df = snowpark_session.sql('SELECT CURRENT_USER, CURRENT_DATE')
  return results_df.collect()

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = retrieve_current_user_and_date
  , return_type = StringType()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_RETRIEVE_CURRENT_USER_AND_DATE'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_RETRIEVE_CURRENT_USER_AND_DATE()
''').show()
