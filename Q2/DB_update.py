# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
import json,time,sys
from collections import OrderedDict
import thread
import boto3
import numpy
from boto3.dynamodb.conditions import Key,Attr
import decimal

sys.path.append('utils')

import tripupdate,vehicle,alert,mtaUpdates_zoli,aws

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def checkFilter(x, filtList):
	try:
		if int(x) in filtList:
			return True
	except:
		pass
	return False

def checkTime(x):
	try:
		if int(x) > 0:
			return True
		else:
			return False
	except:
		return False



def mtaupdate(i, dynamodb, dataFeed, table, filterList):
	i = 0
	while(1):	
		Data = dataFeed.getTripUpdates()
		#print Data
		for item in Data:
			if type(item) == tripupdate.tripupdate:
				if checkFilter(item.routeId, filterList) and checkTime(item.timeStamp):	
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
							'tripId': item.tripId,
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
			
			if type(item) == vehicle.vehicle:
				if (table.get_item(TableName = 'mtaData', Key={'tripId': item.tripid}, AttributesToGet=['timeStamp'])) != '':
					table.update_item(
						Key={
						'tripId': item.tripid
						},
						UpdateExpression = "set currentStopId = :val1, currentStopStatus = :val2, vehicleTimeStamp = :val3",
						ExpressionAttributeValues = {
						':val1': item.currentStopId,
						':val2': item.currentStopStatus,
						':val3': item.vehicleTimestamp
	
						}
				)
				else:
					pass
			
 



	
		#print 'done'
		time.sleep(30)
		#i = i+1		
		#print str(i)
		




def ScanTable(e, dynamodb, dataFeed, table):
	
	while(1):	
		result = []
		count = 0
		print("Printing MTA Records")
        	response = table.scan()
		for i in response['Items']:
			'''
			if not i['timeStamp']:
				try:
					table.delete_item(
					Key={
					'tripId':i['tripId']
					}
					)
					count = count + 1
				except:
					pass
			'''
			if (abs(int(time.time()) - i['timeStamp']) > 60) or (i['timeStamp'] == None ):
				try:
					table.delete_item(
					Key={
					'tripId':i['tripId']
					}
					)
					count = count + 1
				except:
					pass
		print count
		time.sleep(60)

def updateDB(tableName, filterList):

	dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tableName)
	dataFeed = mtaUpdates_zoli.mtaUpdates('12767375c36f0eb36fa22526df437929')
	#try:
	thread.start_new_thread( mtaupdate, ("Thread-1", dynamodb, dataFeed, table, filterList))#, kwargs={'dynamodb':dynamodb, 'dataFeed':dataFeed, 'table':table, 'filterList':filterList} )
	thread.start_new_thread( ScanTable, ("Thread-2",dynamodb, dataFeed, table))#, kwargs={'dynamodb':dynamodb, 'dataFeed':dataFeed, 'table':table} )
	#except:
	#	print "Error: unable to start thread"
	#	print sys.exc_info()[0]
	while True:
		pass


