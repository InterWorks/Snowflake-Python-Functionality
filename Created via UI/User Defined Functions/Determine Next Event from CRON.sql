
-- Simple UDF accept a CRON and an optional reference timestamp
-- then return the next event timestamp

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION determine_next_event_from_cron(
      CRON_SCHEDULE_STRING string
    , REFERENCE_TIMESTAMP timestamp
  )
  returns timestamp
  language python
  runtime_version = '3.8'
  packages = ('croniter')
  handler = 'determine_next_event_from_cron_py'
as
$$
from croniter import croniter
from datetime import datetime

def determine_next_event_from_cron_py(
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
$$
;

------------------------------------------------------------------
-- Testing

select determine_next_event_from_cron('00 03 * * 4', '2022-10-5 10:00:15');
select determine_next_event_from_cron('00 03 * * 4', current_timestamp());
select determine_next_event_from_cron('00 03 * * 4', NULL);

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
  , determine_next_event_from_cron(CRON_SCHEDULE_STRING, REFERENCE_TIMESTAMP) as NEXT_EVENT
from test_values
;

