
-- Simple procedure to multiply two input integers together

-- This particular example would be more useful as a 
-- UDF instead of a procedure, however it is useful
-- for simply demonstrating the construction of 
-- a stored procedure in Snowflake

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE multiply_two_integers_together(
      INPUT_INT_1 int
    , INPUT_INT_2 int
  )
  returns int not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'multiply_together_py'
as
$$
def multiply_together_py(
    snowpark_session
  , input_int_py_1: int
  , input_int_py_2: int
  ):
  return input_int_py_1*input_int_py_2
$$
;

------------------------------------------------------------------
-- Testing

call multiply_two_integers_together(3, 7);

