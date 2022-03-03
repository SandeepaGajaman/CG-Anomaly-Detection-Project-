import json
import pymongo
import pandas as pd  
import datetime as dt
from pytz import timezone
from fbprophet import Prophet
from holidays import WEEKEND, HolidayBase

def lambda_handler(event, context):
    #Forecast
    run_forecast_COMBANK_PO()
    clear_cg_trans_data()

    return {
        'statusCode': 200,
        'body': json.dumps('!!! Code Executed Successfully !!!')
    }

def get_data(APP_ID):
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')

    ##Specify the database to be used
    db = client.papyrus

    ##Specify the collection to be used
    col = db.cg_trans_data

    # Query Cargills transaction data data
    cursor = col.find({"appId": APP_ID}).sort("requestedDate", 1 )

    # Query data into dataframe
    df = pd.DataFrame(list(cursor))

    #Close the connection
    client.close()

    #Return App ID
    return df

def insert_forecated_data(data):

    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')

    ##Specify the database to be used
    db = client.papyrus

    ##Specify the collection to be used
    col = db.cg_forecasted_data

    #Insert data
    col.insert(data)

    #Close the connection
    client.close()

def run_forecast_COMBANK_PO():

    #Get data for APPID
    df = get_data('COMBANK_PO')

    #Rename columns
    df = df.rename(columns={'requestedDate': 'ds', 'requestCount': 'y'})

    #DateTimeIndex
    df['ds'] = pd.DatetimeIndex(df['ds'])

    ##Prophet

    #Create model
    m = Prophet(interval_width=0.95, daily_seasonality=50, weekly_seasonality=False, yearly_seasonality=False)

    #Fit model
    m.fit(df)

    #Future timeframe fore forecast
    future = m.make_future_dataframe(periods=96, freq='15MIN', include_history=False)
    forecast = m.predict(future)

    #Insert the App ID into dataframe 
    forecast['appID'] = 'COMBANK_PO'
    
    #Get the forecasted result
    forecast = forecast[['appID','ds','yhat']]

    #Create the json document to insert 
    forecast = json.loads(forecast.to_json(orient = 'records', date_format = 'iso'))
    
    #Insert the document to DB
    insert_forecated_data(forecast)
        
def clear_cg_trans_data():
    ##Getting the Time Zone
    time_zone = timezone('Asia/Colombo')
    srilanka_time = dt.datetime.now(time_zone)

    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')

    ##Specify the database to be used
    db = client.papyrus

    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Getting the Current Time fo Query
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.replace(hour=0,minute=0,second=0,microsecond=0)
    reduce_days = dt.timedelta(days=121) # 121 days = 4 months
    
    ##Getting 15 minutes past time for Query
    past_time = current_time - reduce_days

    ##Format the time
    zeros = '.000'
    past_time = str(past_time)
    past_time = past_time.replace('+05:30', '') + zeros

    # Clear raw data
    col.delete_many({"requestedDate":{"$lt": past_time}})

    #Close the connection
    client.close()
    
    #Log
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print('cg_trans_data collection cleard at ' + current_time)
