
# Simple UDF accept a CRON and an optional reference timestamp
# then return the next event timestamp

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### Import the required modules 
from croniter import croniter
from datetime import datetime

def determine_next_event_from_cron(
      cron_schedule_string: str
    , reference_timestamp: datetime
  ):
  if reference_timestamp is None :
    reference_timestamp = datetime.now()
  try : 
    cron_schedule = croniter(cron_schedule_string, reference_timestamp)
    next_run = cron_schedule.get_next(datetime)
    return next_run
  except Exception as e:
    return None

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StringType, TimestampType
snowpark_session.add_packages('croniter')

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = determine_next_event_from_cron
  , return_type = TimestampType()
  , input_types = [StringType(), TimestampType()]
  , is_permanent = True
  , name = 'SNOWPARK_DETERMINE_NEXT_EVENT_FROM_CRON'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  select snowpark_determine_next_event_from_cron('00 03 * * 4', '2022-10-5 10:00:15')
''').show()

snowpark_session.sql('''
  select snowpark_determine_next_event_from_cron('00 03 * * 4', current_timestamp())
''').show()

snowpark_session.sql('''
  select snowpark_determine_next_event_from_cron('00 03 * * 4', NULL)
''').show()

snowpark_session.sql('''
  with test_values as (
    select 
        $1::string as CRON_SCHEDULE_STRING
      , $2::timestamp as REFERENCE_TIMESTAMP
    from values 
        ('0,25,40,50 8-18 * * 1-5', '2022-10-05 10:06:15')
      , ('0 5 * * *', '2022-10-05 10:06:15')
      , ('*/15 * * * 2-5', '2022-10-05 10:06:15')
      , ('0 6 * * 1-2', CURRENT_TIMESTAMP)
      , ('0 10,16 5-15 * *', NULL)
      , ('00 21 * * *', '2022-10-05 10:06:15')
  )
  select 
      CRON_SCHEDULE_STRING
    , REFERENCE_TIMESTAMP
    , snowpark_determine_next_event_from_cron(CRON_SCHEDULE_STRING, REFERENCE_TIMESTAMP) as NEXT_EVENT
  from test_values
''').show()
