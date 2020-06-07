#!/usr/bin/python3
import pika
import json

from pprint import pprint
from collections import Counter
from errorcode import Errorcode
from config import rmq_user, rmq_password

# RabbitMQ server connection
rmq_virtual_host = '/'
rmq_input_exchange = 'exchange.logdata.input'
rmq_source_queue = 'queue.logdata.input'
rmq_completed_exchange = 'exchange.logdata.output'

rmq_credentials = pika.PlainCredentials(rmq_user, rmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', virtual_host=rmq_virtual_host, credentials=rmq_credentials))
channel = connection.channel()

occuredErrorsList = []

print(' [*] Waiting for logs. To exit press CTRL+C')

# Creates a warning message based on error code.
def createWarningMessage(object):
    message = {"errorCode": object.errorCode, "Warning": f"Warning, this error has occured {object.errorOccurences} times."}
    return message

# If the number of errors match the alert it sends warning to output.
def errorCounter(occuredErrorList):
    for item in occuredErrorList:
        if item.errorAlert == item.errorOccurences:
            if item.errorWarning == False:
                message = createWarningMessage(item)
                channel.basic_publish(exchange=rmq_completed_exchange,
                        routing_key='',
                        body=json.dumps(message))
                item.errorWarning = True

# Receives the message and count number of times error has occured.
# Once finished, sends an acknowledgement back to sender.
def callbackMessage(ch, method, properties, body):
    print("[x] Received message")

    logdata = json.loads(body)

    found = False
    for item in occuredErrorsList:
        if item.errorCode == logdata['errorCode']:
            item.errorOccurences += 1
            found = True
            pprint(f"Occurences: {item.errorOccurences} on error code {item.errorCode}")
            break
    if not found:
        errorData = Errorcode(logdata)
        occuredErrorsList.append(errorData)

    errorCounter(occuredErrorsList)
    print("[x] Work done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

# When new message is received, call the function callbackMessage.
channel.basic_consume(
    queue=rmq_source_queue, on_message_callback=callbackMessage)

channel.start_consuming()