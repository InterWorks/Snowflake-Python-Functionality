
-- Procedure to multiply all members of an input array of integers
-- with another input integer

-- This particular example would be more useful as a 
-- UDF instead of a procedure, however it is useful
-- for simply demonstrating the construction of 
-- a stored procedure in Snowflake

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE multiply_all_integers_in_array(
      INPUT_ARRAY array
    , INPUT_INT int
  )
  returns array not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'multiply_integers_in_array_py'
as
$$

# First define a function which multiplies two integers together
def multiply_together_py(
    a: int
  , b: int
  ):
  return a*b

# Define main function which maps multiplication function
# to all members of the input array
def multiply_integers_in_array_py(
    snowpark_session
  , input_list_py: list
  , input_int_py: int
  ):
  # Use list comprehension to apply the function multiply_together_py
  # to each member of the input list
  return [multiply_together_py(i, input_int_py) for i in input_list_py]
$$
;

------------------------------------------------------------------
-- Testing

call multiply_all_integers_in_array([1,2,3,4,5,6,7,8,9], 3);