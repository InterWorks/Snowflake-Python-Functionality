
-- Simple UDTF to create a calendar table
-- taking inputs for the start and end date

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION CALENDAR_TABLE (
      START_DATE DATE
    , END_DATE DATE
  )
  returns TABLE (
        DATE DATE
      , YEAR INT
      , MONTH INT
      , MONTH_NAME TEXT
      , MONTH_NAME_SHORT TEXT
      , DAY INT
      , DAY_NAME TEXT
      , DAY_NAME_SHORT TEXT
      , DAY_OF_WEEK INT
      , DAY_OF_YEAR INT
      , WEEK_OF_YEAR INT
      , ISO_YEAR INT
      , ISO_WEEK INT
      , ISO_DAY INT
    )
  language python
  runtime_version = '3.8'
  handler = 'calendar_table'
as
$$

# Import required standard library
from datetime import date, timedelta

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
    
$$
;

------------------------------------------------------------------
-- Testing

select * from table(CALENDAR_TABLE('2015-04-01'::date, '2021-03-31'::date));
