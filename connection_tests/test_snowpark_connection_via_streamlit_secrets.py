
# Test connection to Snowpark leveraging Streamlit secrets

## Import required function
from snowpark.snowpark_session_builder import build_snowpark_session_via_streamlit_secrets

## Generate Snowpark session
snowpark_session_via_streamlit_secrets = build_snowpark_session_via_streamlit_secrets()

## Simple commands to test the connection by listing the databases in the environment
df_test_via_streamlit_secrets = snowpark_session_via_streamlit_secrets.sql('SHOW DATABASES')
df_test_via_streamlit_secrets.show()
