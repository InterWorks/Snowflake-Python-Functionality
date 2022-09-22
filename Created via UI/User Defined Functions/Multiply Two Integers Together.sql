
-- Simple UDF to multiply two input integers together

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION multiply_two_integers_together(
      INPUT_INT_1 int
    , INPUT_INT_2 int
  )
  returns int not null
  language python
  runtime_version = '3.8'
  handler = 'multiply_together_py'
as
$$
def multiply_together_py(
    input_int_py_1: int
  , input_int_py_2: int
  ):
  return input_int_py_1*input_int_py_2
$$
;

------------------------------------------------------------------
-- Testing

select multiply_two_integers_together(3, 7);

select 
    uniform(1, 100, random())::int as MY_INT_1
  , uniform(1, 100, random())::int as MY_INT_2
  , multiply_two_integers_together(MY_INT_1, MY_INT_2)
from (table(generator(rowcount => 100)))
;

