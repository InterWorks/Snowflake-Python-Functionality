
-- UDF to generate a Snowflake-compatible key pair for authentication

------------------------------------------------------------------
-- Create the UDF

CREATE OR REPLACE FUNCTION generate_key_pair()
  returns variant not null
  language python
  runtime_version = '3.8'
  packages = ('cryptography')
  handler = 'generate_key_pair_py'
as
$$

# Import the required modules 
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# Define main function which generates a key pair
def generate_key_pair_py():
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
$$
;

------------------------------------------------------------------
-- Testing

select 
    generate_key_pair() as key_pair
  , key_pair:"private_key"::string as private_key
  , key_pair:"public_key"::string as public_key
;

select 
    generate_key_pair() as key_pair
  , key_pair:"private_key"::string as private_key
  , key_pair:"public_key"::string as public_key
from (table(generator(rowcount => 100)))
;

