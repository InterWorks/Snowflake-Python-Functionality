
-- Execute a metadata command and drop the results into a table

------------------------------------------------------------------
-- Create the stored procedure

CREATE OR REPLACE PROCEDURE SP_METADATA_COMMAND_TO_TABLE(
    METADATA_COMMAND varchar
  , DESTINATION_TABLE varchar
)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'execute_metadata_command_into_table'
  execute as caller -- Must execute as caller to support the SHOW command
as
$$


##################################################################################################################################################
#### Validate inputs 
##################################################################################################################################################

#########################################################################
## Error if any required variables are not provided

def validate_variables_list(REQUIRED_VARIABLES_LIST: list) :

  FUNCTION_RESULT = ''
  FUNCTION_RESULT_FLAG = False

  ## Use list comprehension to filter list of required variables to those with an empty or None value
  INVALID_REQUIRED_VARIABLES_LIST = [x["VARIABLE_NAME"] for x in REQUIRED_VARIABLES_LIST if x["VARIABLE_VALUE"] == None or len(x["VARIABLE_VALUE"]) == 0]

  ## If any failed members are found, combine the error messages 
  if len(INVALID_REQUIRED_VARIABLES_LIST) > 0 :
    REQUIRED_VARIABLES_VALIDATION_ERROR_MESSAGES = [f"Failed: {VARIABLE_NAME} parameter must be populated" for VARIABLE_NAME in INVALID_VARIABLES_LIST]
    REQUIRED_VARIABLES_VALIDATION_ERROR_MESSAGES_COMBINED = ',\n'.join(REQUIRED_VARIABLES_VALIDATION_ERROR_MESSAGES)
    FUNCTION_RESULT = REQUIRED_VARIABLES_VALIDATION_ERROR_MESSAGES_COMBINED
    FUNCTION_RESULT_FLAG = True

  return [FUNCTION_RESULT, FUNCTION_RESULT_FLAG]

##################################################################################################################################################
#### Define and execute main function
##################################################################################################################################################

def execute_metadata_command_into_table(snowpark_session, METADATA_COMMAND: str, DESTINATION_TABLE: str) :

  ## Define RESULT variable that will be output at the end,
  ## or output if an caught error occurs
  RESULT = ""

  ## Define list of required variables
  REQUIRED_VARIABLES_LIST = [
      {
          "VARIABLE_NAME": 'METADATA_COMMAND'
        , "VARIABLE_VALUE": METADATA_COMMAND
      }
    , {
          "VARIABLE_NAME": 'DESTINATION_TABLE'
        , "VARIABLE_VALUE": DESTINATION_TABLE
      }
  ]

  try:
    ## Execute function to test required variables
    [REQUIRED_VARIABLES_RESULT, REQUIRED_VARIABLES_RESULT_FLAG] = validate_variables_list(REQUIRED_VARIABLES_LIST)

    ## Halt procedure early if variables test fails
    if REQUIRED_VARIABLES_RESULT_FLAG == True :
      RESULT += REQUIRED_VARIABLES_RESULT
      return RESULT
      
  except Exception as err :
    return err
  
  ## Attempt to execute the metadata command and insert the results into the destination table
  try :

    ## Read the command into a Snowflake dataframe
    METADATA_RESULT_DF = snowpark_session.sql(METADATA_COMMAND)

    ## Write the results of the dataframe into a target table
    METADATA_RESULT_DF.write.mode("overwrite").save_as_table(DESTINATION_TABLE)
    
    RESULT += f"Succeeded: Results inserted into table {DESTINATION_TABLE}"
  
  except Exception as err :
    return err
    
  return RESULT
    
$$
;

/*
------------------------------------------------------------------
-- Testing

CALL SP_METADATA_COMMAND_TO_TABLE(
    'SHOW DATABASES'              -- METADATA_COMMAND varchar
  , 'MY_TEST_TABLE'               -- DESTINATION_TABLE varchar
)
;

SELECT * FROM MY_TEST_TABLE;

*/
