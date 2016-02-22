# *********************************************************************************************
# Usage python mta.py

import json,time,csv,sys
#from DB_update import updateDB
import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
#import aws
import time





DYNAMODB_TABLE_NAME = "mtaData"





# prompt
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







def main():
	prompt()
	x = command()
	if x == 1:
		plantrip()
	elif x == 2:
		exit()
	elif x == 3:
		exit()
	

	return 0


def plantrip():
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
	print stations_dict['127S']		

if __name__ == "__main__":
    main()
    

   

        
