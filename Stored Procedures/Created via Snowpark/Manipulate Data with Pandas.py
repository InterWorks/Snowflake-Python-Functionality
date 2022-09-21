
# Convert a Snowflake dataframe to a pandas one, and opposite

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session
import snowflake.snowpark

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the Stored Procedure

# Import required modules
import pandas

# Define function
def manipulate_data_with_pandas(
    snowpark_session: snowflake.snowpark.Session
  , origin_table: str
  , destination_table: str
  , filter_field: str
  , filter_value: str
  ):

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

##################################################################
## Register Stored Produre in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType
snowpark_session.add_packages('snowflake-snowpark-python', 'pandas')

### Upload Stored Produre to Snowflake
snowpark_session.sproc.register(
    func = manipulate_data_with_pandas
  , return_type = StringType()
  , input_types = [StringType(), StringType(), StringType(), StringType()]
  , is_permanent = True
  , name = 'SNOWPARK_MANIPULATE_DATA_WITH_PANDAS'
  , replace = True
  , stage_location = '@SPROC_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  CALL SNOWPARK_MANIPULATE_DATA_WITH_PANDAS('MY_ORIGIN_TABLE', 'MY_DESTINATION_TABLE', 'owner', 'CONSULTANT')
''').show()
