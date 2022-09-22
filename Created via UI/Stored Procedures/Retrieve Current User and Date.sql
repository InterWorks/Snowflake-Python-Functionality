
-- Simple procedure to execute a SQL command that
-- contains the current user and date

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE retrieve_current_user_and_date()
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'retrieve_current_user_and_date_py'
as
$$
def retrieve_current_user_and_date_py(snowpark_session):

  ## Execute the query into a Snowflake dataframe
  results_df = snowpark_session.sql('SELECT CURRENT_USER, CURRENT_DATE')
  return results_df.collect()
$$
;

------------------------------------------------------------------
-- Testing

call retrieve_current_user_and_date();

