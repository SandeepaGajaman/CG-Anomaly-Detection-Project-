import pymongo
import psycopg2
import datetime as dt
from pytz import timezone

##Getting the Time Zone
time_zone = timezone('Asia/Colombo')
srilanka_time = dt.datetime.now(time_zone)

def lambda_handler(event, context):
    get_data_from_cgdom()

    return {
        'statusCode': 200,
        'body': json.dumps('!! Code Executed Successfully !!')
    }

def get_data_from_cgdom():
    conn_string_cgdom = "dbname='cgdb' port='0000' user='cguser' password='pw' host='cgdbhost'"
    #~Connect to PostgreSQL from Python
    conncgdom = psycopg2.connect(conn_string_cgdom)
    #~Get Cursor Object from Connection
    curcgdom = conncgdom.cursor()
    #~Set DB Schema 
    curcgdom.execute("SET search_path TO 'cgdom'")
    
    #~Get the Current Time
    current_time = srilanka_time.replace(second=0, microsecond=0)
    reduce_minutes = dt.timedelta(minutes=15)
    
    #~Getting 15 minutes past time for Query
    past_time = current_time - reduce_minutes
    
    #~Format the time for query
    current_time = str(current_time)
    current_time = current_time.replace('+05:30', '')
    past_time = str(past_time)
    past_time = past_time.replace('+05:30', '')

    #~Custom SQL Query
    sql_query = "SELECT APPID, COUNT(TRANSACTIONID) FROM CGDOM.TRANSACTION WHERE REQ_DATE >= '" + past_time + "' AND REQ_DATE < '" + current_time + "' GROUP BY APPID ORDER BY APPID"
    
    #~Execute the SELECT query using a execute() method
    curcgdom.execute(sql_query)
    #~Extract all rows from a result
    result = curcgdom.fetchall()
    
    #~Get the Query Time
    query_time = srilanka_time.strftime("%Y-%m-%d %H:%M:00.000")

    #~Insert data into the "cg_trans_data" table
    for row in result:
        appid = row[0] 
        count = row[1]
        time = query_time 
        row = {"appId":appid,"requestedDate":time,"requestCount":count}
        insert_record(row)
    
def insert_record(row):
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.papyrus
    
    ##Specify the collection to be used
    col = db.cg_trans_data
    
    ##Inserting real-time processed data
    col.insert(row)

    ##Close the connection
    client.close()
    
