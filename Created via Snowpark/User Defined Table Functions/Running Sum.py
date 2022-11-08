
# UDTF to demonstrate a simple running SUM

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Create the stage and table to store the demo data

# Create the stage if it does not exist
snowpark_session.sql('''
  CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS
''').collect()

'''
This is empty to start, so we
need to upload our "Demo Sales Data.csv"
file into this stage. This is achieved
with the following commands in a local
SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Demo Sales Data.csv' @STG_FILES_FOR_UDTFS;
*/
'''

# View files in stage
snowpark_session.sql('''
  LIST @STG_FILES_FOR_UDTFS;
''').show()

# Create the table of demo data
snowpark_session.sql('''
create or replace table DEMO_SALES_DATA (
    SALE_DATE     DATE
  , CATEGORY      TEXT
  , SUBCATEGORY   TEXT
  , SALES         FLOAT
)
''').collect()

# Ingest data into the table
snowpark_session.sql('''
  copy into DEMO_SALES_DATA
  from '@STG_FILES_FOR_UDTFS/Demo Sales Data.csv'
  file_format = (TYPE = 'CSV' SKIP_HEADER = 1)
''').collect()

# View demo table
snowpark_session.sql('''
  select * from DEMO_SALES_DATA
''').show()

##################################################################
## Define the class for the UDTF

# Define handler class
class generate_running_sum :

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create initial running sum variable at zero
    self._running_sum = 0
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_measure: float
    ) :

    # Increment running sum with data
    # from the input row
    new_running_sum = self._running_sum + input_measure
    self._running_sum = new_running_sum

    yield(new_running_sum,)

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import FloatType

### Define output schema
output_schema = StructType([
      StructField("RUNNING_SUM", FloatType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = generate_running_sum
  , output_schema = output_schema
  , input_types = [FloatType()]
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_RUNNING_SUM'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  select * 
  from DEMO_SALES_DATA
    , table(
        SNOWPARK_GENERATE_RUNNING_SUM(SALES) 
        over (
          partition by CATEGORY, SUBCATEGORY
          order by SALE_DATE asc
        )
      )
''').show()


