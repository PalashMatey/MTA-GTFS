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

def uptown_downtown():
	input = raw_input("Going uptown or downtown? ")
	if input not in set(['uptown', 'downtown']):
		return uptown_downtown()
	return input

def prompt2():
	stations = {'242':'101S' ,'238':'103S' ,'231':'104S' ,'Marble Hill-225':'106S' ,
		    '215':'107S' ,'207':'108S' ,'Dyckman':'109S' ,'191':'110S' ,
                    '181':'111S', '168-Washington Heights':'112S' ,'157':'113S' , '145':'114S' ,
		    '137-City College':'115S' ,'125':'116S' ,'116':'117S' ,'110':'118S' ,'103':'119S' }        
	print "Please select source station: "
	print "242 \n 238 \n 231 \n Marble Hill-225 \n 215 \n 207 \n Dyckman \n 191 \n 181 \n 168-Washington Heights \n 157 \n 145 \n 137-City College \n 125 \n 116 \n 110 \n 103"
	input = raw_input('Enter the station code: ')
	#for input not in stations:
	#	return prompt()
	return stations[input]

def prompt3():
	stations = {'242':'101N' ,'238':'103N' ,'231':'104N' ,'Marble Hill-225':'106N' ,
                    '215':'107N' ,'207':'108N' ,'Dyckman':'109N' ,'191':'110N' ,
                    '181':'111N', '168-Washington Heights':'112N' ,'157':'113N' , '145':'114N' ,
                    '137-City College':'115N' ,'125':'116N' ,'116':'117N' ,'110':'118N' ,'103':'119N' }
        print "Please select destination station: "
        print "242 \n 238 \n 231 \n Marble Hill-225 \n 215 \n 207 \n Dyckman \n 191 \n 181 \n 168-Washington Heights \n 157 \n 145 \n 137-City College \n 125 \n 116 \n 110 \n 103"
        input = raw_input('Enter the station code: ')
        #for input not in stations:
        #       return prompt()
        return stations[input]

def findTrain():

	direction = uptown_downtown()
	if direction == "downtown":
		origin = prompt2()
		dest = '127S'
		switch = '120S'

		train1 = {}
		train23 = {}
		response = table.scan()
		for i in response['Items']:
	
			try:
				if origin in i['futureStopData']:
				#if '117S' in i['futureStopData']:
					train1[int( i['futureStopData'][origin]['arrivalTime'] )] = i['tripid']
				
				if switch in i['futureStopData'] and i['routeId'] in set(['2','3']):
					train23[int( i['futureStopData'][switch]['arrivalTime'] )] = i['tripid']
			except:
				pass

		print "All local trains thru 96th: " 
		print [value for key, value in train1.items()]

		next1_id = train1[min(train1)]
		for i in response['Items']:
			if i['tripid'] == next1_id:
				my1train = i
				TSarrivalTime1 = int(i['futureStopData'][dest]['arrivalTime'])
				NSixarrivalTime1 = int(i['futureStopData'][switch]['arrivalTime'])
				break

		train23 = {key:value for key, value in train23.items() if key  > NSixarrivalTime1}
		next23_id = train23[min(train23)]
	
		print "\n All express trains thru 96th: "
		print [value for key, value in train23.items()]

		for i in response['Items']:
			if i['tripid'] == next23_id:
				my23train = i
				TSarrivalTime23 = int(i['futureStopData'][dest]['arrivalTime'])
				break
		print "\nLocal Train: " + str(next1_id)
		print "Express Train: " + str(next23_id)
		print "Estimated travel time to/from 42nd on" +  str(next1_id)+" train " + str(round((TSarrivalTime1-time.time())/60.0,2))
		print "Estimated travel time to/from 42nd switching to  2 or 3 Train: " + str(round((TSarrivalTime23-time.time())/60.0,2))
	
		if TSarrivalTime23 <= TSarrivalTime1:
			print "Switch" 
			temp= "Switch "+ "Difference in time: " + str(round(abs(TSarrivalTime23-TSarrivalTime1)/60.0,2))+ "mins"
			client.publish(TopicArn=topic_arn, Message=str(temp))
		else:
			print "Stay"
			client.publish(TopicArn=topic_arn, Message='Stay')
	
	elif direction == "uptown":
                origin = '127N'
                dest = prompt3()
                switch = '120N'

		train1 = {}
                train23 = {}
                response = table.scan()
                for i in response['Items']:
			try:
				if origin in i['futureStopData'] and i['routeId'] in set(['2','3']):
					train23[int( i['futureStopData'][origin]['arrivalTime'] )] = i['tripid']
				elif origin in i['futureStopData'] and i['routeId'] == '1':
					train1[int( i['futureStopData'][origin]['arrivalTime'] )] = i['tripid']
			except:
				continue

		next1_id = train1[min(train1)]
		next23_id = train23[min(train23)]

		for i in response['Items']:
			if i['tripid'] == next1_id:
				my1train = i
			elif i['tripid'] == next23_id:
				my23train = i

		arrivalTime1 = int(my1train['futureStopData'][switch]['arrivalTime'])
		arrivalTime23 = int(my23train['futureStopData'][switch]['arrivalTime'])
		fin1 =  int(my1train['futureStopData'][dest]['arrivalTime'])
		
		newTrain1 = {}
		for i in response['Items']:
			try:
				if switch in i['futureStopData'] and i['routeId'] == '1' and int(i['futureStopData'][switch]['arrivalTime']) >= arrivalTime23:
					newTrain1[ int( i['futureStopData'][dest]['arrivalTime'] ) ] = i['tripid']
			except:
				continue
		fin23 = int(min(newTrain1))
		print "All local trains thru 96th: "
                print [value for key, value in train1.items()]
		
		print "\n All express trains thru 96th: "
                print [value for key, value in train23.items()]


		print "\nLocal train: " + str(next1_id)
		print "Express train: " + str(next23_id)
		print "Estimated travel time to/from 42nd on" +  str(next1_id)+" train " + str(round((fin1-time.time())/60.0,2))
                print "Estimated travel time to/from 42nd switching to  2 or 3 Train: " + str(round((fin23-time.time())/60.0,2))

		if arrivalTime23 < arrivalTime1:
			temp =  "Take the express and switch at 96th"
			client.publish(TopicArn=topic_arn, Message=str(temp))
		else:
			print "Take the local"
			client.publish(TopicArn=topic_arn, Message="Take the local")

def ScanTable(e):
	
	while(1):	
		result = []
		count = 0
		#print("Printing MTA Records")
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
		#print count
        	#print 'Done deleting old items in the table'
		time.sleep(60)

def subscribe():

	try:
		number = raw_input("Please input phone number: ")
		client.subscribe(TopicArn=topic_arn, Protocol='sms', Endpoint=number)
	except Exception as e:
		print "\nFailed to subscribe user:"
		print str(e)

def prompt():
    print ""
    print ">Available Commands are : "
    print "1. plan trip"
    print "2. subscribe to messages"
    print "3. exit"

def main():
	prompt()
	try:
		input = int(raw_input("Input command: "))
	except:
		main()
	if input not in set([1,2,3]):
		main()

	if input == 1:
		findTrain()
		main()

	elif input == 2:
		subscribe()
		main()

	elif input == 3:
		raise SystemExit


if __name__ == "__main__":
	
	print "Initializing..."
	thread.start_new_thread( mtaupdate, ("Thread-1", ) )
	thread.start_new_thread( ScanTable, ("Thread-2", ) )
	time.sleep(10)
	main()
	



