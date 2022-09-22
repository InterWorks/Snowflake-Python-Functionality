
# UDF to generate a Snowflake-compatible key pair for authentication

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Define the function for the UDF

### Import the required modules 
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

### Define main function which generates a Snowflake-compliant key pair
def generate_key_pair():
  keySize = 2048
  
  key = rsa.generate_private_key(public_exponent=65537, key_size=keySize)
  
  privateKey = key.private_bytes(
      crypto_serialization.Encoding.PEM,
      crypto_serialization.PrivateFormat.PKCS8,
      crypto_serialization.NoEncryption()
  )
  privateKey = privateKey.decode('utf-8')
  
  publicKey = key.public_key().public_bytes(
      crypto_serialization.Encoding.PEM,
      crypto_serialization.PublicFormat.SubjectPublicKeyInfo
  )
  publicKey = publicKey.decode('utf-8')
  
  key_pair = {
      "private_key" : privateKey
    , "public_key" : publicKey
  }

  return key_pair

##################################################################
## Register UDF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import VariantType
snowpark_session.add_packages('cryptography')

### Upload UDF to Snowflake
snowpark_session.udf.register(
    func = generate_key_pair
  , return_type = VariantType()
  , input_types = []
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_KEY_PAIR'
  , replace = True
  , stage_location = '@UDF_STAGE'
)

##################################################################
## Testing

snowpark_session.sql('''
  SELECT SNOWPARK_GENERATE_KEY_PAIR() as key_pair
    , key_pair:"private_key"::string as private_key
    , key_pair:"public_key"::string as public_key
''').show()

snowpark_session.sql('''
  select 
      SNOWPARK_GENERATE_KEY_PAIR() as key_pair
    , key_pair:"private_key"::string as private_key
    , key_pair:"public_key"::string as public_key
  from (table(generator(rowcount => 100)))
''').show()
