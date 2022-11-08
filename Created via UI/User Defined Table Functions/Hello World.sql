
-- Simple UDTF to create a table stating "Hello World" over two rows

------------------------------------------------------------------
-- Create the UDTF

CREATE OR REPLACE FUNCTION HELLO_WORLD ()
  returns TABLE (ID INT, NAME TEXT)
  language python
  runtime_version = '3.8'
  handler = 'simple_table'
as
$$

class simple_table :
  
  def process(self) :
    '''
    Enter Python code here that 
    executes for each input row.
    This ends with a set of yield
    clauses that output tuples
    '''
    yield (1, 'Hello')
    yield (2, 'World')
    
    '''
    Alternatively, this may end with
    a single return clause containing
    an iterable of tuples, for example:
    
    return [
        (1, 'Hello')
      , (2, 'World')
    ]
    '''
    
$$
;

------------------------------------------------------------------
-- Testing

select * from table(HELLO_WORLD());
