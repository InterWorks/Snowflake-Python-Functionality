
------------------------------------------------------------------
-- Create the stored procedure to backup the Snowflake share

-- Create the stored procedure
CREATE OR REPLACE PROCEDURE BACKUP_SNOWFLAKE_SHARED_DATABASE()
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'backup_snowflake_shared_database'
  execute as caller
as
$$

# Import module for inbound Snowflake session
from snowflake.snowpark import session as snowpark_session

# Import required modules
from datetime import datetime

# Define function to backup a view as a table with CTAS
def backup_view_as_table(
      snowpark_session: snowpark_session
    , source_view_metadata: dict
    , destination_database: str
  ):

  ## Create FQNs
  source_view_FQN = f'"{source_view_metadata["TABLE_CATALOG"]}"."{source_view_metadata["TABLE_SCHEMA"]}"."{source_view_metadata["TABLE_NAME"]}"'
  destination_table_FQN = f'"{destination_database}"."{source_view_metadata["TABLE_SCHEMA"]}"."{source_view_metadata["TABLE_NAME"]}"'
  
  ## Determine comment
  source_comment = source_view_metadata["COMMENT"]
  destination_comment = 'NULL'
  if source_comment and len(source_comment) > 0 :
    source_comment_as_string = source_comment.replace("'", "\\'")
    destination_comment = f"'{source_comment_as_string}'"

  ## Execute SQL to create a new table via CTAS
  ## using ''' for a multi-line string input
  snowpark_session.sql(f'''
      CREATE SCHEMA IF NOT EXISTS "{destination_database}"."{source_view_metadata["TABLE_SCHEMA"]}"
    ''').collect()
  
  ## Execute SQL to create a new table via CTAS
  ## using ''' for a multi-line string input
  snowpark_session.sql(f'''
      CREATE TABLE {destination_table_FQN}
      AS
      SELECT * FROM {source_view_FQN}
    ''').collect()
    
  ## Execute SQL to set the comment
  snowpark_session.sql(f'''
      ALTER TABLE {destination_table_FQN}
        SET COMMENT = {destination_comment}
    ''').collect()
  
  return

# Define function to create the destination database
def create_destination_database(
      snowpark_session: snowpark_session
  ):

  ## Determine process timestamp
  now = datetime.now()
  processed_timestamp = now.strftime("%Y_%m_%d")

  ## Determine destination database
  destination_database = f'BACKUP_SNOWFLAKE_SHARE_{processed_timestamp}'

  ## Create destination database
  ## using ''' for a multi-line string input
  snowpark_session.sql(f'''
      CREATE OR REPLACE DATABASE "{destination_database}"
    ''').collect()
  
  return destination_database

# Define function to retrieve view metadata from information schema
def retrieve_view_metadata_from_information_schema(
      snowpark_session: snowpark_session
  ):
  
  ## Retrieve stage overview details
  ## using ''' for a multi-line string input
  sf_df_view_metadata = snowpark_session.sql(f'''
      SELECT * FROM SNOWFLAKE.INFORMATION_SCHEMA.VIEWS
      WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' 
    ''')

  df_view_metadata = sf_df_view_metadata.to_pandas()

  view_metadata_list = df_view_metadata.to_dict('records')
  
  return view_metadata_list
  
# Define main function to backup the SNOWFLAKE shared database
def backup_snowflake_shared_database(
      snowpark_session: snowpark_session
  ):

  ## Execute function to create and return the destination database
  destination_database = create_destination_database(snowpark_session=snowpark_session)

  ## Execute function to get list of view metadata
  view_metadata_list = retrieve_view_metadata_from_information_schema(snowpark_session=snowpark_session)

  ## Loop over views in list
  for view_metadata in view_metadata_list :

    ### Execute function to backup a view as a table with CTAS
    backup_view_as_table(
          snowpark_session = snowpark_session
        , source_view_metadata = view_metadata
        , destination_database = destination_database
      )
  
  return 'Complete'
$$
;

------------------------------------------------------------------
-- Testing

call BACKUP_SNOWFLAKE_SHARED_DATABASE();
