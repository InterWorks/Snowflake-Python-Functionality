
# Test connection to Snowpipe leveraging Streamlit secrets

## Import required function
from snowpark.snowpipe_ingest_manager_builder import build_snowpipe_ingest_manager_via_streamlit_secrets

## If desired, populate a target pipe name to test the connection to
target_pipe_name = None

## Generate Snowpark session
snowpipe_ingest_manager_via_parameters_json = build_snowpipe_ingest_manager_via_streamlit_secrets(target_pipe_name=target_pipe_name)

### Simple commands to test the connection by listing the databases in the environment
try :
  snowpipe_response = snowpipe_ingest_manager_via_parameters_json.ingest_files([])
  assert(snowpipe_response['responseCode'] == 'SUCCESS')

  print('Test successful')
except Exception as e:
  if 'Message: Specified object does not exist or not authorized. Pipe not found' in e.message :
    ## In this case, our test is successful as we have authenticated.
    ## This has failed anyway because we have entered a None pipe,
    ## which is our only option if this is to be generic
    print('Authentication successful, however the pipe itself was not found')
  else :
    print('Error identified:')
    print(e)
    