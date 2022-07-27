
-- Simple procedure to multiply an input integer by 3

-- This particular example would be more useful as a 
-- UDF instead of a procedure, however it is useful
-- for simply demonstrating the construction of 
-- a stored procedure in Snowflake

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE multiply_integer_by_three(INPUT_INT int)
  returns int not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'multiply_by_three_py'
as
$$
def multiply_by_three_py(snowpark_session, input_int_py: int):
  return input_int_py*3
$$
;

------------------------------------------------------------------
-- Testing

call multiply_integer_by_three(20);