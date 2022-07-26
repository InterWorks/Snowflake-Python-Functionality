
-- UDF to generate a fake name using faker

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION generate_fake_name()
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('faker')
  handler = 'generate_fake_name_py'
as
$$

# Import the required modules 
from faker import Faker

# Define main function which generates a fake name
def generate_fake_name_py():
  fake = Faker()
  return fake.name()
$$
;

------------------------------------------------------------------
-- Testing

select generate_fake_name();

select 
    generate_fake_name()
from (table(generator(rowcount => 100)))
;
