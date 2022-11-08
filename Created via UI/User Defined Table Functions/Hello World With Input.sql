
-- Simple UDTF to create a table stating "Hello World" 
-- over one or two rows, depending on the input

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION HELLO_WORLD_WITH_INPUT (ROW_COUNT INT)
  returns TABLE (ID INT, NAME TEXT)
  language python
  runtime_version = '3.8'
  handler = 'simple_table_from_input'
as
$$

class simple_table_from_input :
  
  def process(self, row_count: int) :

    # Leverage an if statement to output
    # a different result depending on the input
    if row_count is None or row_count not in [1, 2] :
      return None
    elif row_count == 1 :
      return [(1, 'Hello World')]
    elif row_count == 2 :
      return [
          (1, 'Hello')
        , (2, 'World')
      ]
    
$$
;

------------------------------------------------------------------
-- Testing

select * from table(HELLO_WORLD_WITH_INPUT(1));
select * from table(HELLO_WORLD_WITH_INPUT(2));
select * from table(HELLO_WORLD_WITH_INPUT(3));
