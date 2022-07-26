
-- UDF to multiply all members of an input array of integers
-- with another input integer

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION multiply_all_integers_in_array(
      INPUT_ARRAY array
    , INPUT_INT int
  )
  returns array not null
  language python
  runtime_version = '3.8'
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
    input_list_py: list
  , input_int_py: int
  ):
  # Use list comprehension to apply the function multiply_together_py
  # to each member of the input list
  return [multiply_together_py(i, input_int_py) for i in input_list_py]
$$
;

------------------------------------------------------------------
-- Testing

select multiply_all_integers_in_array([1,2,3,4,5,6,7,8,9], 3);

select 
    array_construct(
        uniform(1, 100, random())::int
      , uniform(1, 100, random())::int
      , uniform(1, 100, random())::int
      , uniform(1, 100, random())::int
      , uniform(1, 100, random())::int
    ) as MY_ARRAY
  , uniform(1, 100, random())::int as MY_INT
  , multiply_all_integers_in_array(MY_ARRAY, MY_INT)
from (table(generator(rowcount => 100)))
;
