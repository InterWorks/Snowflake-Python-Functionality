
-- Procedure to generate a Snowflake-compatible key pair for authentication
-- and apply it for a user, rotating the previous key

------------------------------------------------------------------
-- Create the procedure

CREATE OR REPLACE PROCEDURE generate_key_pair_for_user(INPUT_USERNAME STRING)
  returns string not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'cryptography')
  handler = 'generate_key_pair_for_user_py'
  execute as caller
as
$$

# Import the required modules 
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import re

# Define function which generates a Snowflake-compliant key pair
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

def retrieve_current_public_key_for_user(snowpark_session, username: str):
  
  # Retrieve user's current key
  # using ''' for a multi-line string input
  # and formatted string literals to leverage variables
  desc_user_df = snowpark_session.sql(f'''
    DESCRIBE USER "{username}"
  ''').collect()

  rsa_public_key_value = [row["value"] for row in desc_user_df if row["property"] == 'RSA_PUBLIC_KEY'][0]

  return rsa_public_key_value

def rotate_public_key_for_user(snowpark_session, username: str, old_public_key: str, new_public_key: str):
  
  # Parse the new_public_key into the format preferred by Snowflake
  # by stripping the start and end clauses
  regex_pattern = "-----BEGIN PUBLIC KEY-----\\n(.*)\\n-----END PUBLIC KEY-----\\n?"
  result = re.search(regex_pattern, new_public_key, re.S)
  new_public_key_parsed = result.group(1)  
  
  # Rotate user's current key
  # using ''' for a multi-line string input
  # and formatted string literals to leverage variables
  # and .collect() to ensure execution on Snowflake
  if old_public_key  is not None \
    and len(old_public_key) > 0 \
    and old_public_key != 'null' \
    :
    snowpark_session.sql(f'''
      ALTER USER "{username}"
        SET RSA_PUBLIC_KEY_2 = '{old_public_key}'
    ''').collect()

  snowpark_session.sql(f'''
    ALTER USER "{username}"
      SET RSA_PUBLIC_KEY = '{new_public_key_parsed}'
  ''').collect()

  return 0

# Define main function to generate and rotate the key pair for a user
def generate_key_pair_for_user_py(snowpark_session, username: str):
  new_key_pair = generate_key_pair()
  new_public_key = new_key_pair["public_key"]
  new_private_key = new_key_pair["private_key"]

  old_public_key = retrieve_current_public_key_for_user(snowpark_session, username)
  
  rotate_public_key_for_user(snowpark_session, username, old_public_key, new_public_key)

  return new_private_key

$$
;

------------------------------------------------------------------
-- Testing

call generate_key_pair_for_user('chris.hastie');
