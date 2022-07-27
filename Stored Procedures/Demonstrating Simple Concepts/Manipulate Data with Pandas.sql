
-- Convert a Snowflake dataframe to a pandas one, and opposite

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE manipulate_data_with_pandas(
      INPUT_ORIGIN_TABLE STRING
    , INPUT_DESTINATION_TABLE STRING
    , INPUT_FILTER_FIELD STRING
    , INPUT_FILTER_VALUE STRING
  )
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'pandas')
  handler = 'manipulate_data_with_pandas_py'
as
$$

import pandas

def manipulate_data_with_pandas_py(snowpark_session, origin_table: str, destination_table: str, filter_field: str, filter_value: str):

  # Read the origin table into a Snowflake dataframe
  results_df_sf = snowpark_session.table(origin_table)

  # Convert the Snowflake dataframe into a Pandas dataframe
  results_df_pd = results_df_sf.to_pandas()

  # Filter the Pandas dataframe to databases where the field matches the value
  results_df_pd_filtered = results_df_pd[results_df_pd[filter_field] == filter_value]

  # Convert the filtered Pandas dataframe into a Snowflake dataframe
  results_df_sf_filtered = snowpark_session.create_dataframe(results_df_pd_filtered)
  
  # Write the results of the dataframe into a target table
  results_df_sf_filtered.write.mode("overwrite").save_as_table(destination_table)
    
  return f"Succeeded: Results inserted into table {destination_table}"
$$
;

------------------------------------------------------------------
-- Testing

call manipulate_data_with_pandas('MY_ORIGIN_TABLE', 'MY_DESTINATION_TABLE', 'owner', 'CONSULTANT');

SELECT * FROM MY_DESTINATION_TABLE;
