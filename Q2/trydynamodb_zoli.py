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
sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates_zoli,aws
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
dynamodb = boto3.resource('dynamodb')
dataFeed = mtaUpdates_zoli.mtaUpdates('12767375c36f0eb36fa22526df437929')

table = dynamodb.Table('MTAfeed')
def mtaupdate(i):
	global dataFeed
	i = 0
	while(1):	
		Data = dataFeed.getTripUpdates()
		#print Data
		for item in Data:
			#print item
			#print '0'
			#tripupdate = item		 
			#vehicle = item
			#alert  = Data[-1]
			if type(item) == tripupdate.tripupdate:
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
			if type(item) == vehicle.vehicle:
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
				if item.currentStopId == ''
 



	
		#print 'done'
		time.sleep(30)
		#i = i+1		
		#print str(i)
		




def ScanTable(e):
	
	while(1):	
		result = []
		count = 0
		print("Printing MTA Records")
        	response = table.scan(
        	)
		for i in response['Items']:
			if (abs(int(time.time()) - i['timeStamp']) > 60) or (i['timeStamp'] == 
None ):
				try:
					table.delete_item(
					Key={
					'tripid':i['tripid']
					}
					)
					count = count + 1
				except:
					pass
					# Error Handling		
				#count = count + 1
				#result.append(i['tripid'])
		#print 'Number if entries are: '
		print count
		#print 'Returning the result: '
		#for item in result:
                #	table.delete_item(
                #	Key={
                #	'tripid': item,
                #	 }
                #	)
        	print 'Done deleting old items in the table'
		time.sleep(60)

try:
   thread.start_new_thread( mtaupdate, ("Thread-1", ) )
   thread.start_new_thread( ScanTable, ("Thread-2", ) )
except:
   print "Error: unable to start thread"
while True:
	pass

