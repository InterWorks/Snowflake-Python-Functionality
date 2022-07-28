
-- Stored procedure to demonstrate leveraging a non-standard library called xlrd

-- Details on xlrd can be found here:
-- https://xlrd.readthedocs.io/en/latest/

------------------------------------------------------------------
-- Create the stage

-- Create the stage if it does not exist
CREATE STAGE IF NOT EXISTS STG_FILES_FOR_STORED_PROCEDURES;

-- View files in stage
LIST @STG_FILES_FOR_STORED_PROCEDURES;

-- This is empty to start, so we
-- need to upload our xlrd
-- directory into this stage. We do not
-- compress the file so that we can keep
-- the example function simple. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/lib/Site-Packages/xlrd/xldate.py' @STG_FILES_FOR_STORED_PROCEDURES/xlrd AUTO_COMPRESS = FALSE ;
*/

-- View files in stage
LIST @STG_FILES_FOR_STORED_PROCEDURES; 

-- This file deliberately has spaces in the name
-- so that we can also demonstrate how to handle that

-- View specific file in stage
LIST '@STG_FILES_FOR_STORED_PROCEDURES/xlrd/'; 

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE leverage_external_library(INPUT_INT integer)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  imports = ('@STG_FILES_FOR_STORED_PROCEDURES/xlrd/xldate.py')
  handler = 'leverage_external_library_py'
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

# Define main function which leverages the mapping
def leverage_external_library_py(snowpark_session, input_int_py: int):
  return xldate.xldate_as_datetime(input_int_py, 0)

$$
;

------------------------------------------------------------------
-- Testing

call leverage_external_library(500);
