import boto3
import mraa
import time
import thread
from math import log

# Boto3 Init
client = boto3.client('sns', 'us-east-1')

topic_arn='arn:aws:sns:us-east-1:602386368437:Lab4Q1'
message_subject='CurrentTemp'
#account_ID=['602386368437']
#action_name=['AddPermission']

#client.add_permission(TopicArn=topic_arn, Label='Temp SNS', AWSAccountId=account_ID, ActionName=action_name)

# Temperature Sensor Init
tempPin = 1
tempSensor = mraa.Aio(tempPin)

def TempRead(e):

        global tempSensor, client, topic_arn, message_subject

        # Define constants
        B = 4275
        R0 = 100000.0

        while True:

                # Get raw value
                raw = tempSensor.read()

                # Calculate
                R = 1023.0/raw - 1.0
                R = R0*R
                temp = 1.0/(log(R/R0)/B+1/298.15)-273.15
                print temp
                try:
                    	client.publish(TopicArn=topic_arn, Message=str(temp))
                except e:
                        print e

                time.sleep(15)

try:
   thread.start_new_thread( TempRead , ("Thread-1", ) )
except:
   print "Error: unable to start thread"
while True:
	pass


