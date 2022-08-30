
-- Simple UDF to multiply an input integer by 3

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION multiply_integer_by_three(INPUT_INT int)
  returns int not null
  language python
  runtime_version = '3.8'
  handler = 'multiply_by_three_py'
as
$$
def multiply_by_three_py(input_int_py: int):
  return input_int_py*3
$$
;

------------------------------------------------------------------
-- Testing

select multiply_integer_by_three(20);

select 
    uniform(1, 100, random())::int as MY_INT
  , multiply_integer_by_three(MY_INT)
from (table(generator(rowcount => 100)))
;

