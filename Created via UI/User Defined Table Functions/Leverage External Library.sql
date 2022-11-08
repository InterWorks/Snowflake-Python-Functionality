
-- UDTF to demonstrate leveraging a non-standard library called xlrd

-- Details on xlrd can be found here:
-- https://xlrd.readthedocs.io/en/latest/

------------------------------------------------------------------
-- Create the stage and table to store the demo data

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS;

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This is empty to start, so we
-- need to upload our xlrd
-- directory into this stage. We do not
-- compress the file so that we can keep
-- the example function simple. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/xlrd/xldate.py' @STG_FILES_FOR_UDTFS/xlrd AUTO_COMPRESS = FALSE ;
*/

-- View files in stage
LIST @STG_FILES_FOR_UDTFS;

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_UDTFS/xlrd/'; 

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION LEVERAGE_EXTERNAL_LIBRARY (
      INPUT_INT INTEGER
  )
  returns TABLE (
      EXCEL_TIMESTAMP TIMESTAMP
    )
  language python
  runtime_version = '3.8'
  imports = ('@STG_FILES_FOR_UDTFS/xlrd/xldate.py')
  handler = 'leverage_external_library'
as
$$

# Import the required modules 
import sys

# Retrieve the Snowflake import directory
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

# Import the required external modules using the importlib.util library
import importlib.util
module_spec = importlib.util.spec_from_file_location('xldate', import_dir + 'xldate.py')
xldate = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(xldate)

# Define handler class
class leverage_external_library :
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_int: int
    ) :

    ### Apply the mapping to retrieve the mapped value
    xl_datetime = xldate.xldate_as_datetime(input_int, 0)

    yield(xl_datetime,)
  
$$
;
 
------------------------------------------------------------------
-- Testing

with test_values as (
  select
      uniform(1, 100, random())::int as MY_INT
  from (table(generator(rowcount => 100)))
)
select
    *
from test_values
  , table(LEVERAGE_EXTERNAL_LIBRARY(MY_INT))
;
