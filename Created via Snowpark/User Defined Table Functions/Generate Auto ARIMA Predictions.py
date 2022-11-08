
# UDTF to generate and train an auto ARIMA 
# machine learning model then test predictions

##################################################################
## Establish Snowpark session leveraging locally-stored JSON file

### Import required function
from interworks_snowpark.interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_via_parameters_json as build_snowpark_session

### Generate Snowpark session
snowpark_session = build_snowpark_session()

##################################################################
## Create the stage and table to store the demo data

# Create the stage if it does not exist
snowpark_session.sql('''
  CREATE STAGE IF NOT EXISTS STG_FILES_FOR_UDTFS
''').collect()

'''
This is empty to start, so we
need to upload our "Demo Sales Data.csv"
file into this stage. This is achieved
with the following commands in a local
SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key"

PUT 'FILE://C:/My/Path/To/Supporting Files/Demo Sales Data.csv' @STG_FILES_FOR_UDTFS;
*/
'''

# View files in stage
snowpark_session.sql('''
  LIST @STG_FILES_FOR_UDTFS;
''').show()

# Create the table of demo data
snowpark_session.sql('''
create or replace table DEMO_SALES_DATA (
    SALE_DATE     DATE
  , CATEGORY      TEXT
  , SUBCATEGORY   TEXT
  , SALES         FLOAT
)
''').collect()

# Ingest data into the table
snowpark_session.sql('''
  copy into DEMO_SALES_DATA
  from '@STG_FILES_FOR_UDTFS/Demo Sales Data.csv'
  file_format = (TYPE = 'CSV' SKIP_HEADER = 1)
''').collect()

# View demo table
snowpark_session.sql('''
  select * from DEMO_SALES_DATA
''').show()

##################################################################
## Define the class for the UDTF

# Import modules
import pandas
import pmdarima
from datetime import date

# Define handler class
class generate_auto_arima_predictions :

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create empty list to store inputs
    self._data = []
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_date: date
      , input_measure: float
    ) :

    # Ingest rows into pandas DataFrame
    data = [input_date, input_measure]
    self._data.append(data)

  ## Define end_partition method that acts
  ## on full partition after rows are processed
  def end_partition(self) :

    # Convert inputs to DataFrame
    df_input = pandas.DataFrame(data=self._data, columns=["DATE", "MEASURE"])

    # Determine test and train splits of the data
    # leverage an 80:20 ratio.
    train_data, test_data = pmdarima.model_selection.train_test_split(df_input["MEASURE"], test_size=0.2)

    # Create Auto Arima model
    model = pmdarima.auto_arima(
          train_data
        , test='adf'
        , max_p=3, max_d=3, max_q=3
        , seasonal=True, m=12, max_P=3
        , max_D=2, max_Q=3, trace=True
        , error_action='ignore'
        , suppress_warnings=True
        , stepwise=True
      )
    
    # Convert train and test values to dataframes
    df_train = pandas.DataFrame(data=train_data, columns=["MEASURE_TRAIN"])
    df_test = pandas.DataFrame(data=test_data, columns=["MEASURE_TEST"])

    # Generate in-sample predictions
    pred_train = model.predict_in_sample(dynamic=False) # works only with auto-arima
    df_train = pandas.DataFrame(
        data=pandas.to_numeric(pred_train)
      , columns=["PREDICTION_TRAIN"]
    )

    # Generate predictions on test data
    pred_test = model.predict(n_periods=len(test_data), dynamic=False)
    df_test = pandas.DataFrame(
        data=pandas.to_numeric(pred_test)
      , columns=["PREDICTION_TEST"]
    )
    # Adjust test index to align with
    # the end of the training data
    df_test.index += len(df_train)

    # Combine test and train prediction values with original
    df_output = pandas.concat([df_input, df_train, df_test], axis = 1) \
      [["DATE", "MEASURE", "PREDICTION_TRAIN", "PREDICTION_TEST"]]
                              
    # Output the result
    return list(df_output.itertuples(index=False, name=None))

    '''
    Alternatively, the output could be returned
    by iterating through the rows and yielding them
    for index, row in df_output.iterrows() :
      yield(row[0], row[1], row[2], row[3])
    '''

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import DateType, FloatType
snowpark_session.add_packages('pandas', 'pmdarima')

### Define output schema
output_schema = StructType([
      StructField("DATE", DateType())
    , StructField("MEASURE", FloatType())
    , StructField("PREDICTION_TRAIN", FloatType())
    , StructField("PREDICTION_TEST", FloatType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = generate_auto_arima_predictions
  , output_schema = output_schema
  , input_types = [DateType(), FloatType()]
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_AUTO_ARIMA_PREDICTIONS'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

# Initial example demonstrates how
# current warehouses will run out of memory.
# Snowflake's High Memory Virtual Warehouses are
# currently in Private Preview and should resolve this.
snowpark_session.sql('''
  select * 
  from DEMO_SALES_DATA
    , table(
        SNOWPARK_GENERATE_AUTO_ARIMA_PREDICTIONS(SALE_DATE, SALES) 
        over (
          partition by CATEGORY, SUBCATEGORY
          order by SALE_DATE asc
        )
      )
''').show()

# By aggregating and filtering to a smaller
# dataset, it is possible to execute the UDTF.
snowpark_session.sql('''
  with aggregated as (
    select
        DATE_TRUNC('MONTH', SALE_DATE) AS SALE_MONTH 
      , CATEGORY
      , SUM(SALES) as SALES
    from DEMO_SALES_DATA
    where year(SALE_DATE) >= 2005
    group by
        SALE_MONTH
      , CATEGORY
  )
  select
      CATEGORY
    , DATE
    , MEASURE
    , PREDICTION_TRAIN
    , PREDICTION_TEST
  from aggregated
    , table(
        SNOWPARK_GENERATE_AUTO_ARIMA_PREDICTIONS(SALE_MONTH, SALES) 
        over (
          partition by CATEGORY
          order by SALE_MONTH asc
        )
      )
''').show()
