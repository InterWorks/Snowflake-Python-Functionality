
# Simple UDTF to create a calendar table
# taking inputs for the start and end date

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the class for the UDTF

# Import required standard library
from datetime import date, timedelta

# Define the class for the UDTF
class calendar_table :
  
  def process(
        self
      , start_date: date
      , end_date: date
    ) :

    # Leverage list comprehension to generate dates between start and end
    list_of_dates = [start_date+timedelta(days=x) for x in range((end_date-start_date).days + 1)]

    # Leverage list comprehension to create tuples of desired date details from list of dates
    list_of_date_tuples = [
        (
            date
          , date.year
          , date.month
          , date.strftime("%B")
          , date.strftime("%b")
          , date.day
          , date.strftime("%A")
          , date.strftime("%a")
          , date.strftime("%w")
          , date.strftime("%j")
          , date.strftime("%W")
          , date.strftime("%G")
          , date.strftime("%V")
          , date.strftime("%u")
        ) 
        for date in list_of_dates
      ]

    return list_of_date_tuples

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import DateType, IntegerType, StringType

### Define output schema
output_schema = StructType([
      StructField("DATE", DateType())
    , StructField("YEAR", IntegerType())
    , StructField("MONTH", IntegerType())
    , StructField("MONTH_NAME", StringType())
    , StructField("MONTH_NAME_SHORT", StringType())
    , StructField("DAY", IntegerType())
    , StructField("DAY_NAME", StringType())
    , StructField("DAY_NAME_SHORT", StringType())
    , StructField("DAY_OF_WEEK", IntegerType())
    , StructField("DAY_OF_YEAR", IntegerType())
    , StructField("WEEK_OF_YEAR", IntegerType())
    , StructField("ISO_YEAR", IntegerType())
    , StructField("ISO_WEEK", IntegerType())
    , StructField("ISO_DAY", IntegerType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = calendar_table
  , output_schema = output_schema
  , input_types = [DateType(), DateType()]
  , is_permanent = True
  , name = 'SNOWPARK_CALENDAR_TABLE'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT * FROM TABLE(SNOWPARK_CALENDAR_TABLE('2015-04-01'::date, '2021-03-31'::date))
''').show()


