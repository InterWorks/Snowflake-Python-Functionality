
# UDTF to generate and train a scikit learn
# linear regression model and predict results

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
from sklearn.linear_model import LinearRegression
from datetime import date

# Define handler class
class generate_linear_regression_predictions :

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create empty list to store inputs
    self._data = []
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_year: date
      , input_measure: float
      , input_periods_to_forecast: int
    ) :

    # Ingest rows into pandas DataFrame
    data = [input_year, 'ACTUAL', input_measure]
    self._data.append(data)
    self._periods_to_forecast = input_periods_to_forecast

  ## Define end_partition method that acts
  ## on full partition after rows are processed
  def end_partition(self) :

    # Convert inputs to DataFrame
    df_input = pandas.DataFrame(data=self._data, columns=["YEAR", "TYPE", "MEASURE"])

    # Determine inputs for linear regression model
    # x = pandas.DatetimeIndex(df_input["DATE"]).year.to_numpy().reshape(-1, 1)
    x = df_input["YEAR"].to_numpy().reshape(-1, 1)
    y = df_input["MEASURE"].to_numpy()

    # Create linear regression model
    model = LinearRegression().fit(x, y)

    # Determine forecast range
    periods_to_forecast = self._periods_to_forecast
    # Leverage list comprehension to generate desired years
    list_of_years_to_predict = [df_input["YEAR"].iloc[-1] + x + 1 for x in range(periods_to_forecast)]
    
    prediction_input = [[x] for x in list_of_years_to_predict]
    
    # Generate predictions and create a df
    predicted_values = model.predict(prediction_input)
    predicted_values_formatted = pandas.to_numeric(predicted_values).round(2).astype(float)
    df_predictions = pandas.DataFrame(
        data=predicted_values_formatted
      , columns=["MEASURE"]
    )

    # Create df for prediction year
    df_prediction_years = pandas.DataFrame(
        data=list_of_years_to_predict
      , columns=["YEAR"]
    )

    # Create df for prediction type
    prediction_type_list = ["PREDICTION" for x in list_of_years_to_predict]
    df_prediction_type = pandas.DataFrame(
        data=prediction_type_list
      , columns=["TYPE"]
    )

    # Combine predicted dfs into single df
    df_predictions_combined = pandas.concat([df_prediction_years, df_prediction_type, df_predictions], axis = 1) \
      [["YEAR", "TYPE", "MEASURE"]]

    # Adjust test index to align with
    # the end of the training data
    df_predictions.index += len(df_input)

    # Combine predicted values with original
    df_output = pandas.concat([df_input, df_predictions_combined], axis = 0) \
      [["YEAR", "TYPE", "MEASURE"]]

    # Output the result
    return list(df_output.itertuples(index=False, name=None))

    '''
    Alternatively, the output could be returned
    by iterating through the rows and yielding them
    for index, row in df_output.iterrows() :
      yield(row[0], row[1], row[2])
    '''

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import IntegerType, StringType, FloatType
snowpark_session.add_packages('pandas', 'scikit-learn')

### Define output schema
output_schema = StructType([
      StructField("YEAR", IntegerType())
    , StructField("TYPE", StringType())
    , StructField("MEASURE", FloatType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = generate_linear_regression_predictions
  , output_schema = output_schema
  , input_types = [IntegerType(), FloatType(), IntegerType()]
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_LINEAR_REGRESSION_PREDICTIONS'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)

##################################################################
## Testing

# Aggregate our data by year to keep things simple
snowpark_session.sql('''
  with aggregated as (
    select
        YEAR(SALE_DATE) AS SALE_YEAR 
      , CATEGORY
      , SUBCATEGORY
      , ROUND(SUM(SALES), 2) as SALES
    from DEMO_SALES_DATA
    group by
        YEAR(SALE_DATE)
      , CATEGORY
      , SUBCATEGORY
  )
  select
      CATEGORY
    , SUBCATEGORY
    , YEAR
    , TYPE
    , MEASURE
  from aggregated
    , table(
        GENERATE_LINEAR_REGRESSION_PREDICTIONS(SALE_YEAR, SALES, 5) 
        over (
          partition by CATEGORY, SUBCATEGORY
          order by SALE_YEAR asc
        )
      )
  where YEAR > 2015
''').show()
