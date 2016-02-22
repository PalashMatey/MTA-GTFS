# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
import json,time,sys
from collections import OrderedDict
import thread
import boto3
import numpy
from boto3.dynamodb.conditions import Key,Attr
import decimal,csv
sys.path.append('utils')
import tripupdate,vehicle,alert,mtaUpdates_zoli,aws
import datetime
client = boto3.client('sns', 'us-east-1')
topic_arn='arn:aws:sns:us-east-1:602386368437:Lab4Q1'
message_subject='Switch or Not'
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
dynamodb = boto3.resource('dynamodb')
dataFeed = mtaUpdates_zoli.mtaUpdates('12767375c36f0eb36fa22526df437929')
table = dynamodb.Table('MTAfeed')
def prompt():
    print ""
    print ">Available Commands are : "
    print "1. plan trip"
    print "2. subscribe to messages"
    print "3. exit"

def command():
        x = int(raw_input('Enter a command: '))
        if x == 0 or x == 1 or x == 3:
                print 'Accepted Input'
                return x
        else:
                print '\nInvalid command!'
                prompt()
                command()

def mtaupdate(i):
	global dataFeed
	filter_list = [1,2,3]
	ids_station = []
        name_station = []
        with open('stops.csv', 'rb') as f:
                stations = csv.reader(f,delimiter = ',')
                for row in stations:
                        ids_station.append(row[0])
                        name_station.append(row[2])
        

	stations_dict = {}
        for i in range(len(ids_station)):
                stations_dict[ids_station[i]] = name_station[i]
	i = 0
	while(1):	
		Data = dataFeed.getTripUpdates()
			#print Data
		for item in Data:
			#print '0'
			#tripupdate = item		 
			#vehicle = item
			#alert  = Data[-1]
			if type(item) == tripupdate.tripupdate:
				if ((item.routeId == '1' or item.routeId == '2' or item.routeId == '3') and  item.timeStamp):

					try:
						table.update_item(
                                        		Key={
                                                		'tripid': item.tripid
                                        			},
                                		UpdateExpression = "set routeId = :val1, startDate = :val2, direction = :val3, futureStopData = :val4, timeStamp = :val5",
                                		ExpressionAttributeValues = {
                                        		':val1': item.routeId,
                                        		':val2': item.startDate,
                                        		':val3': item.direction,
							':val4': item.futureStops,
							':val5': item.timeStamp
						}
						)
					except:
						table.put_item(
	                              			Item={
								'tripid': item.tripId,
								'routeId': item.routeId,
								'startDate': item.startDate,
								'direction': item.direction,
								'currentStopId': 'Unassigned',
								'currentStopStatus': 'Unassigned',
								'vehicleTimeStamp': 'Unassigned',
								'futureStopData': item.futureStops,
								'timeStamp': item.timeStamp
								#'timeStamp': time.time(),
								}
							)
			
			if type(item) == vehicle.vehicle and item.currentStopId and item.currentStopStatus and item.vehicleTimestamp:
				table.update_item(
					Key={
						'tripid': item.tripid
					},
				UpdateExpression = "set currentStopId = :val1, currentStopStatus = :val2, vehicleTimeStamp = :val3",
				ExpressionAttributeValues = {
					':val1': item.currentStopId,
					':val2': item.currentStopStatus,
					':val3': item.vehicleTimestamp

				}		
				)

				#if item.currentStopId == '120S':
				#	print item.tripid
				#	print item.currentStopStatus
					

	
		#print 'done'
		time.sleep(5)
		#i = i+1		
		#print str(i)

		
def findTrain():
	train1 = {}
	train23 = {}
	response = table.scan()
	for i in response['Items']:
	
		try:
			if '117S' in i['futureStopData']:
				train1[int( i['futureStopData']['117S']['arrivalTime'] )] = i['tripid']
			if '120S' in i['futureStopData'] and i['routeId'] in set(['2','3']):
				train23[int( i['futureStopData']['120S']['arrivalTime'] )] = i['tripid']
		except:
			pass

	print "All local trains thru 96th: " 
	print [value for key, value in train1.items()]

	next1_id = train1[min(train1)]
	for i in response['Items']:
		if i['tripid'] == next1_id:
			my1train = i
			TSarrivalTime1 = int(i['futureStopData']['127S']['arrivalTime'])
			NSixarrivalTime1 = int(i['futureStopData']['120S']['arrivalTime'])
			break

	train23 = {key:value for key, value in train23.items() if key  > NSixarrivalTime1}
	next23_id = train23[min(train23)]
	
	print "\n All express trains thru 96th: "
	print [value for key, value in train23.items()]

	for i in response['Items']:
		if i['tripid'] == next23_id:
			my23train = i
			TSarrivalTime23 = int(i['futureStopData']['127S']['arrivalTime'])
			break
	print "\nLocal Train: " + str(next1_id)
	print "Express Train: " + str(next23_id)
	print "Estimated travel time to 42nd from 116 on 1 Train: " + str(round((TSarrivalTime1-time.time())/60.0,2))
	print "Estimated travel time to 42nd from 116 if you switch to 2 or 3 Train: " + str(round((TSarrivalTime23-time.time())/60.0,2))
	
	if TSarrivalTime23 <= TSarrivalTime1:
		print "Switch" 
		temp= "Switch "+ "Difference in time: " + str(round(abs(TSarrivalTime23-TSarrivalTime1)/60.0,2))+ "mins"
		client.publish(TopicArn=topic_arn, Message=str(temp))
	else:
		print "Stay"
		client.publish(TopicArn=topic_arn, Message='Stay')
	
	
	

def ScanTable(e):
	
	while(1):	
		result = []
		count = 0
		print("Printing MTA Records")
        	response = table.scan()
		for i in response['Items']:
			try:
				if (abs(int(time.time()) - abs(i['timeStamp'])) > 60) or (i['timeStamp'] == None):
				
					table.delete_item(
					Key={
					'tripid':i['tripid']
					}
					)
					count = count + 1
			except:
				pass
		print count
        	print 'Done deleting old items in the table'
		time.sleep(60)


thread.start_new_thread( mtaupdate, ("Thread-1", ) )
thread.start_new_thread( ScanTable, ("Thread-2", ) )
time.sleep(10)
findTrain()

while True:
	pass

