import json
import pymongo
import datetime as dt
import pandas as pd 
import boto3
from pytz import timezone

def lambda_handler(event, context):

    detect_anomaly()
    clear_cg_trans_raw_data()

    return {
        'statusCode': 200,
        'body': json.dumps('anomaly detection success')
    }
    

##Custom defined percentage
constant_percentage = 50
constant_percentage_small_apps = 85

##Getting the Time Zone
time_zone = timezone('Asia/Colombo')
srilanka_time = dt.datetime.now(time_zone)

def clear_cg_trans_raw_data():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_raw_data
    
    ##Getting the Current Time fo Query
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.replace(second=0, microsecond=0)
    reduce_minutes = dt.timedelta(minutes=30)
    
    ##Getting 15 minutes past time for Query
    past_time = current_time - reduce_minutes
    
    ##Format the time
    zeros = '.000'
    past_time = str(past_time)
    past_time = past_time.replace('+05:30', '') + zeros

    # Clear raw data
    col.delete_many({"requestedDate":{"$lt": past_time}})
    
    #Log
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print('cg_trans_raw_data collection cleard at ' + current_time)

    #Close the connection
    client.close()

"""
-----------------
Anomaly Detection
-----------------
"""


def detect_anomaly():
    topicArn = 'arn:aws:sns:EmailTopic'
    snsClient = boto3.client('sns', region_name='ap-southeast-1')
    
    sl_time = dt.datetime.now(time_zone)
    current_time = sl_time.time()
    current_time = current_time.replace(microsecond=0)
    current_time = str(current_time).replace('+05:30', '')
    
    if current_time > '08:00:00' and current_time < '20:00:00':
        print('Peak time - 8 am to 8 pm')
        appIdArr = ['CARG','LBF']
        for i in appIdArr:
            appId = i
            
            if appId == 'CARG':
                print('Processing ' + appId + ' app id')
                noData = get_CARG()
                if noData == 0:
                    srilanka_time = dt.datetime.now(time_zone)
                    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                    noDataMsg = "!!! WARNING !!!\n\nHi Team,\nNo CG transaction data received to the CG from the " + appId + " app id at " + current_time + ". Maybe an anomaly. If this message comes frequently please contact Ops Team.\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                    response = snsClient.publish(TopicArn=topicArn, Message= noDataMsg, Subject="" + appId + " - CG No Payment Transactions")
                else:
                    forecast_value,actual_value,actual_percentage = get_CARG()
                    ##Alerting!
                    if forecast_value > actual_value and actual_percentage > constant_percentage:
                        forecast_value = str(forecast_value)
                        actual_value = str(actual_value)
                        actual_percentage = str(actual_percentage)
                        srilanka_time = dt.datetime.now(time_zone)
                        current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                        alertMessage = "Hi Team,\ndetect an anomaly in " + appId + " at " + current_time + ". Receiving request count is lower than the usual. Please check!! Anomaly Percentage = " + actual_percentage + "% !!!\n\n(Forecasted request count is " + forecast_value + " and actual request count is " + actual_value +")\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                        response = snsClient.publish(TopicArn=topicArn, Message= alertMessage, Subject="" + appId + " - CG Payment Pattern Anomaly")
                    
            if appId == 'LBF':
                print('Processing ' + appId + ' app id')
                noData = get_LBF()
                if noData == 0:
                    srilanka_time = dt.datetime.now(time_zone)
                    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                    noDataMsg = "!!! WARNING !!!\n\nHi Team,\nNo CG transaction data received to the CG from the " + appId + " app id at " + current_time + ". Maybe an anomaly. If this message comes frequently please contact Ops Team.\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                    response = snsClient.publish(TopicArn=topicArn, Message= noDataMsg, Subject="" + appId + " - CG No Payment Transactions")
                else:
                    forecast_value,actual_value,actual_percentage = get_LBF()
                    ##Alerting!
                    if forecast_value > actual_value and actual_percentage > constant_percentage_small_apps:
                        forecast_value = str(forecast_value)
                        actual_value = str(actual_value)
                        actual_percentage = str(actual_percentage)
                        srilanka_time = dt.datetime.now(time_zone)
                        current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                        alertMessage = "Hi Team,\ndetect an anomaly in " + appId + " at " + current_time + ". Receiving request count is lower than the usual. Please check!! Anomaly Percentage = " + actual_percentage + "% !!!\n\n(Forecasted request count is " + forecast_value + " and actual request count is " + actual_value +")\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                        response = snsClient.publish(TopicArn=topicArn, Message= alertMessage, Subject="" + appId + " - CG Payment Pattern Anomaly")
                                           
    else:
        print('Off-peak time - 8 pm to 8 am')
        appIdArr = ['OMOB_CRD','COMBANK_PO']
        for i in appIdArr:
            appId = i
            
            if appId == 'OMOB_CRD':
                print('Processing ' + appId + ' app id')
                noData = get_OMOB()
                if noData == 0:
                    srilanka_time = dt.datetime.now(time_zone)
                    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                    noDataMsg = "!!! WARNING !!!\n\nHi Team,\nNo CG transaction data received to the CG from the " + appId + " app id at " + current_time + ". Maybe an anomaly. If this message comes frequently please contact Ops Team.\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                    response = snsClient.publish(TopicArn=topicArn, Message= noDataMsg, Subject="" + appId + " - CG No Payment Transactions")
                else:
                    forecast_value,actual_value,actual_percentage = get_OMOB()
                    ##Alerting!
                    if forecast_value > actual_value and actual_percentage > constant_percentage:
                        forecast_value = str(forecast_value)
                        actual_value = str(actual_value)
                        actual_percentage = str(actual_percentage)
                        srilanka_time = dt.datetime.now(time_zone)
                        current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                        alertMessage = "Hi Team,\ndetect an anomaly in " + appId + " at " + current_time + ". Receiving request count is lower than the usual. Please check!! Anomaly Percentage = " + actual_percentage + "% !!!\n\n(Forecasted request count is " + forecast_value + " and actual request count is " + actual_value +")\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                        response = snsClient.publish(TopicArn=topicArn, Message= alertMessage, Subject="" + appId + " - CG Payment Pattern Anomaly")

            if appId == 'COMBANK_PO':
                print('Processing ' + appId + ' app id')
                noData = get_COMBANK_PO()
                if noData == 0:
                    srilanka_time = dt.datetime.now(time_zone)
                    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                    noDataMsg = "!!! WARNING !!!\n\nHi Team,\nNo CG transaction data received to the CG from the " + appId + " app id at " + current_time + ". Maybe an anomaly. If this message comes frequently please contact Ops Team.\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                    response = snsClient.publish(TopicArn=topicArn, Message= noDataMsg, Subject="" + appId + " - CG No Payment Transactions")
                else:
                    forecast_value,actual_value,actual_percentage = get_COMBANK_PO()
                    ##Alerting!
                    if forecast_value > actual_value and actual_percentage > constant_percentage:
                        forecast_value = str(forecast_value)
                        actual_value = str(actual_value)
                        actual_percentage = str(actual_percentage)
                        srilanka_time = dt.datetime.now(time_zone)
                        current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                        alertMessage = "Hi Team,\ndetect an anomaly in " + appId + " at " + current_time + ". Receiving request count is lower than the usual. Please check!! Anomaly Percentage = " + actual_percentage + "% !!!\n\n(Forecasted request count is " + forecast_value + " and actual request count is " + actual_value +")\n\nThanks!\n\nBest Regards,\nCG Anomaly Detector"
                        response = snsClient.publish(TopicArn=topicArn, Message= alertMessage, Subject="" + appId + " - CG Payment Pattern Anomaly")
                            
def get_CARG():
    if int(get_forecastData_CARG()) == 0 or int(get_actualData_CARG()) == 0:
        return 0
    else:
        ##Get Forecasted Values
        forecast_value = int(get_forecastData_CARG())
        ##Get Actual Values
        actual_value = int(get_actualData_CARG())
        ##Percentage Calculation
        quotient = actual_value / forecast_value
        actual_percentage = quotient * 100
        actual_percentage = int(actual_percentage)
        actual_percentage = 100 - actual_percentage
        return forecast_value, actual_value, actual_percentage
    
def get_OMOB():
    if int(get_forecastData_OMOB_CRD()) == 0 or int(get_actualData_OMOB_CRD()) == 0:
        return 0
    else:
        ##Get Forecasted Values
        forecast_value = int(get_forecastData_OMOB_CRD())
        ##Get Actual Values
        actual_value = int(get_actualData_OMOB_CRD())
        ##Percentage Calculation
        quotient = actual_value / forecast_value
        actual_percentage = quotient * 100
        actual_percentage = int(actual_percentage)
        actual_percentage = 100 - actual_percentage
        return forecast_value, actual_value, actual_percentage

def get_COMBANK_PO():
    if int(get_forecastData_COMBANK_PO()) == 0 or int(get_actualData_COMBANK_PO()) == 0:
        return 0
    else:
        ##Get Forecasted Values
        forecast_value = int(get_forecastData_COMBANK_PO())
        ##Get Actual Values
        actual_value = int(get_actualData_COMBANK_PO())
        ##Percentage Calculation
        quotient = actual_value / forecast_value
        actual_percentage = quotient * 100
        actual_percentage = int(actual_percentage)
        actual_percentage = 100 - actual_percentage
        return forecast_value, actual_value, actual_percentage
      
def get_LBF():
    if int(get_forecastData_LBF()) == 0 or int(get_actualData_LBF()) == 0:
        return 0
    else:
        ##Get Forecasted Values
        forecast_value = int(get_forecastData_LBF())
        ##Get Actual Values
        actual_value = int(get_actualData_LBF())
        ##Percentage Calculation
        quotient = actual_value / forecast_value
        actual_percentage = quotient * 100
        actual_percentage = int(actual_percentage)
        actual_percentage = 100 - actual_percentage
        return forecast_value, actual_value, actual_percentage

#~ appID - CARG

def get_forecastData_CARG():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_forecasted_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%dT%H:%M:00.000Z")

    ##Custom Query
    query = { 'appID': 'CARG', 'ds': current_time } 
    
    ##Count the retrieving document count
    forecasted_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if forecasted_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process    
    else:
        ##Find the Forecast Result
        forecast_result = col.find(query)
        df = pd.DataFrame(list(forecast_result))
        df = df.drop(columns=['_id','appID','ds'])
        df = df.rename(columns={'yhat': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Forecast Values to Anomaly Detection
        return df
 
def get_actualData_CARG():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:00.000")

    ##Custom Query
    query = { 'appId': 'CARG', 'requestedDate': current_time }
    
    ##Count the retrieving document count
    actual_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if actual_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process
    else:
        ##Find the Actual Result
        actual_result = col.find(query)
        df = pd.DataFrame(list(actual_result))
        df = df.drop(columns=['_id', 'appId','requestedDate'])
        df = df.rename(columns={'requestCount': ''})
        df = pd.to_numeric(df[''], downcast="float")
        #Close the connection
        client.close()
        ##Return Actual Values to Anomaly Detection
        return df

#~ appID - OMOB_CRD

def get_forecastData_OMOB_CRD():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_forecasted_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%dT%H:%M:00.000Z")

    ##Custom Query
    query = { 'appID': 'OMOB_CRD', 'ds': current_time } 
    
    ##Count the retrieving document count
    forecasted_result = col.count_documents(query)

    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if forecasted_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process    
    else:
        ##Find the Forecast Result
        forecast_result = col.find(query)
        df = pd.DataFrame(list(forecast_result))
        df = df.drop(columns=['_id','appID','ds'])
        df = df.rename(columns={'yhat': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Forecast Values to Anomaly Detection
        return df

def get_actualData_OMOB_CRD():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:00.000")

    ##Custom Query
    query = { 'appId': 'OMOB_CRD', 'requestedDate': current_time }
    
    ##Count the retrieving document count
    actual_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if actual_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process
    else:
        ##Find the Actual Result
        actual_result = col.find(query)
        df = pd.DataFrame(list(actual_result))
        df = df.drop(columns=['_id', 'appId','requestedDate'])
        df = df.rename(columns={'requestCount': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Actual Values to Anomaly Detection
        return df
  
#~ appID - COMBANK_PO

def get_forecastData_COMBANK_PO():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_forecasted_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%dT%H:%M:00.000Z")

    ##Custom Query
    query = { 'appID': 'COMBANK_PO', 'ds': current_time } 
    
    ##Count the retrieving document count
    forecasted_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if forecasted_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process    
    else:
        ##Find the Forecast Result
        forecast_result = col.find(query)
        df = pd.DataFrame(list(forecast_result))
        df = df.drop(columns=['_id','appID','ds'])
        df = df.rename(columns={'yhat': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Forecast Values to Anomaly Detection
        return df
      
def get_actualData_COMBANK_PO():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:00.000")

    ##Custom Query
    query = { 'appId': 'COMBANK_PO', 'requestedDate': current_time }
    
    ##Count the retrieving document count
    actual_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if actual_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process
    else:
        ##Find the Actual Result
        actual_result = col.find(query)
        df = pd.DataFrame(list(actual_result))
        df = df.drop(columns=['_id', 'appId','requestedDate'])
        df = df.rename(columns={'requestCount': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Actual Values to Anomaly Detection
        return df
      
#~ appID - LBF

def get_forecastData_LBF():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_forecasted_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%dT%H:%M:00.000Z")

    ##Custom Query
    query = { 'appID': 'LBF', 'ds': current_time } 
    
    ##Count the retrieving document count
    forecasted_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if forecasted_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process    
    else:
        ##Find the Forecast Result
        forecast_result = col.find(query)
        df = pd.DataFrame(list(forecast_result))
        df = df.drop(columns=['_id','appID','ds'])
        df = df.rename(columns={'yhat': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Forecast Values to Anomaly Detection
        return df
      
def get_actualData_LBF():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Get the Current Time fo Query
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:00.000")

    ##Custom Query
    query = { 'appId': 'LBF', 'requestedDate': current_time }
    
    ##Count the retrieving document count
    actual_result = col.count_documents(query)
    
    ##Check whether the programme gets more than a zero document count. 
    ##If yes, terminate the process 
    if actual_result == 0:
        client.close()
        return 0
    ##If receives documents more than a zero, proceeding with the process
    else:
        ##Find the Actual Result
        actual_result = col.find(query)
        df = pd.DataFrame(list(actual_result))
        df = df.drop(columns=['_id', 'appId','requestedDate'])
        df = df.rename(columns={'requestCount': ''})
        df = pd.to_numeric(df[''], downcast="float")
        ##Close the connection
        client.close()
        ##Return Actual Values to Anomaly Detection
        return df
