
-- UDTF to demonstrate a simple running SUM

------------------------------------------------------------------
-- Create the stage and table to store the demo data

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS;

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This is empty to start, so we
-- need to upload our "Demo Sales Data.csv"
-- file into this stage. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Demo Sales Data.csv' @STG_FILES_FOR_UDTFS;
*/

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_UDTFS/Demo Sales Data.csv';

-- Create the table of demo data
create or replace table DEMO_SALES_DATA (
    SALE_DATE     DATE
  , CATEGORY      TEXT
  , SUBCATEGORY   TEXT
  , SALES         FLOAT
)
;

-- Ingest data into the table
copy into DEMO_SALES_DATA
from '@STG_FILES_FOR_UDTFS/Demo Sales Data.csv'
file_format = (TYPE = 'CSV' SKIP_HEADER = 1)
;

-- View demo table
select * from DEMO_SALES_DATA;

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION GENERATE_RUNNING_SUM (
      INPUT_MEASURE FLOAT
  )
  returns TABLE (
      RUNNING_SUM FLOAT
    )
  language python
  runtime_version = '3.8'
  handler = 'generate_running_sum'
as
$$

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
  
$$
;
 
------------------------------------------------------------------
-- Testing

select * 
from DEMO_SALES_DATA
  , table(
      GENERATE_RUNNING_SUM(SALES) 
      over (
        partition by CATEGORY, SUBCATEGORY
        order by SALE_DATE asc
      )
    )
;