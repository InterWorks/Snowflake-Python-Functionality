
# Test connection to Snowpark leveraging locally-stored JSON file

## Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json

## Generate Snowpark session
snowpark_session_via_parameters_json = build_snowpark_session_via_parameters_json()

## Simple commands to test the connection by listing the databases in the environment
df_test_via_parameters_json = snowpark_session_via_parameters_json.sql('SHOW DATABASES')
df_test_via_parameters_json.show()
