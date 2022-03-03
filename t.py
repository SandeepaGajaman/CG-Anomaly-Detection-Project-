import pymongo
import datetime as dt
from pytz import timezone

def lambda_handler(event, context):
    get_raw_data()

    return {
        'statusCode': 200,
        'body': json.dumps('!! Code Executed Successfully !!')
    }


def insert_record(data):
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Inserting real-time processed data
    col.insert(data)
    
    #Log
    time_zone = timezone('Asia/Colombo')
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print("Formatted data was inserted into the cg_trans_data collection at " + current_time)

    ##Close the connection
    client.close()
    
    
def get_raw_data():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_raw_data
    
    ##Getting the Current Time fo Query
    time_zone = timezone('Asia/Colombo')
    srilanka_time = dt.datetime.now(time_zone)
    current_time = srilanka_time.replace(second=0, microsecond=0)
    reduce_minutes = dt.timedelta(minutes=15)
    
    ##Getting 15 minutes past time for Query
    past_time = current_time - reduce_minutes
    
    ##Format the time
    zeros = '.000'
    current_time = str(current_time)
    current_time = current_time.replace('+05:30', '') + zeros
    past_time = str(past_time)
    past_time = past_time.replace('+05:30', '') + zeros
    
    ##Custom Query
    actual_result = col.aggregate([
        {
            "$match" : 
                { "requestedDate" :
                    { "$gte" : past_time , "$lt": current_time }
                }
        },
        {
            "$group" : 
                {
                    "_id" : "$appId",
                    "requestCount" : {"$sum" : 1}
                }
        },
        {
            "$project" :
                {
                    "_id" : 0,
                    "appId" : "$_id",
                    "requestedDate" : current_time,
                    "requestCount" : "$requestCount"
                }
        }])
                     
    ##Pass the data to insert_record(data)
    insert_record(actual_result)
        
    ##Close the connection
    client.close()
