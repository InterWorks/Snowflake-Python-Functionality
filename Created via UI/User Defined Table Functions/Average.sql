
-- UDTF to demonstrate a simple average

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

CREATE OR REPLACE FUNCTION GENERATE_AVERAGE (
      INPUT_MEASURE FLOAT
  )
  returns TABLE (
      AVERAGE FLOAT
    )
  language python
  runtime_version = '3.8'
  handler = 'generate_average'
as
$$

# Define handler class
class generate_average :

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create initial empty list to store values
    self._values = []
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_measure: float
    ) :

    # Increment running sum with data
    # from the input row
    self._values.append(input_measure)

  ## Define end_partition method that acts
  ## on full partition after rows are processed
  def end_partition(self) :
    values_list = self._values

    average = sum(values_list) / len(values_list)

    yield(average,)

$$
;
 
------------------------------------------------------------------
-- Testing

select
    CATEGORY
  , SUBCATEGORY
  , AVERAGE
from DEMO_SALES_DATA
  , table(
      GENERATE_AVERAGE(SALES) 
      over (
        partition by CATEGORY, SUBCATEGORY
        order by SALE_DATE asc
      )
    )
;