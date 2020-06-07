#!/usr/bin/python3
import pika
import time
import random
import json

from config import mongoServer, rmq_user, rmq_password
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint
from errorcode import Errorcode

# MongoDB connection
client = MongoClient(mongoServer)
db=client.thesisProject

mycol = db.logs

# RabbitMQ server connection
rmq_virtual_host = '/'
rmq_input_exchange = 'exchange.logdata.input'
rmq_source_queue = 'queue.logdata.input'
rmq_completed_exchange = 'exchange.logdata.output'

rmq_credentials = pika.PlainCredentials(rmq_user, rmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(
    'localhost', virtual_host=rmq_virtual_host, credentials=rmq_credentials
    ))
channel = connection.channel()

# Generated array of possible error codes.
errorNames = [
    "01276", "01672", "01829", "02841", "02942", "03058", "03093", "03521", "03528", "04823", 
    "05028", "05730", "05777", "06148", "06363", "07545", "07718", "07760", "07775", "08206"
    ]

errorAlerts = [
    21, 16, 16, 20, 26, 10, 21, 15, 18, 4, 
    23, 27, 17, 13, 26, 11, 20, 24, 17, 4
]

# Generate log data based on array and current date and time
def generateLogData():
    now = datetime.now()
    logDate = now.strftime("%Y-%m-%d")
    logTime = now.strftime("%H:%M:%S")

    index = random.randint(0,19)
    errorCode = errorNames[index]
    errorAlert = errorAlerts[index]
    
    return logDate, logTime, errorCode, errorAlert

# Send the log data to the database
def insertToDatabase(logDate, errorCode):
    mydict = {"date": logDate, "error": errorCode}
    x = mycol.insert_one(mydict)
    print("Post added: ")
    pprint(x.inserted_id)

# Constant loop that keeps sending data to message queue and database at random intervals between 1 and 4 seconds.
while True:
# for i in range(20000):
    logDate, logTime, errorCode, errorAlert = generateLogData()

    errorJson = {"errorCode": errorCode,  "errorDescription": "desc", "errorDate": logDate, "errorTime": logTime, "errorAlert": errorAlert, "errorOccurences": 1, "errorWarning": False}
    insertToDatabase(logDate + " " + logTime, errorCode)

    channel.basic_publish(exchange=rmq_input_exchange,
                    routing_key='',
                    body=json.dumps(errorJson))

    interval = 0.3
    time.sleep(interval)

connection.close()