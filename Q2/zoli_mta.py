# *********************************************************************************************
# Usage python mta.py

import json,time,csv,sys
from DB_update import updateDB
import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import aws
import time





DYNAMODB_TABLE_NAME = "mtaData"
filterList = set([1, 2, 3])



def findTrain():
        train1 = {}
        train23 = {}
        response = table.scan()
        for i in response['Items']:
                try:
                        if '117S' in i['futureStopData']:
                                train1[int( i['futureStopData']['117S']['arrivalTime'] )] = i['tripId']
                        if '120S' in i['futureStopData'] and i['routeId'] in set(['2','3']):
                                train23[int( i['futureStopData']['120S']['arrivalTime'] )] = i['tripId']
                except:
                        pass
        next1_id = train1[min(train1)]
        for i in response['Items']:
                if i['tripid'] == next1_id:
                        my1train = i
                        TSarrivalTime1 = int(i['futureStopData']['127S']['arrivalTime'])
                        NSixarrivalTime1 = int(i['futureStopData']['120S']['arrivalTime'])
                        break

        train23 = {key:value for key, value in train23.items() if key  > NSixarrivalTime1}
        next23_id = train23[min(train23)]

        for i in response['Items']:
                if i['tripid'] == next23_id:
                        my23train = i
                        TSarrivalTime23 = int(i['futureStopData']['127S']['arrivalTime'])
                        break

        if TSarrivalTime23 <= TSarrivalTime1:
                print "Switch"
        else:
                print "Stay"



def prompt():
    print ""
    print ">Available Commands are : "
    print "1. plan trip"
    print "2. subscribe to messages"
    print "3. exit"  

def command():
	x = int(raw_input('Enter a command: '))
	if x == 1 or x == 2 or x == 3:
		return x
	else:
		print '\nInvalid command!'
		prompt()
		command()







def main():
	prompt()
	x = command()
	if x == 1:
		updateDB(DYNAMODB_TABLE_NAME, filterList)
		findtrain()
	elif x == 2:
		exit()
	elif x == 3:
		exit()
	

	return 0


if __name__ == "__main__":
    main()
    

   

        
