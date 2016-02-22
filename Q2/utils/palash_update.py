import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict

from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf

import vehicle,alert,tripupdate

class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')
    
    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    
    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}    
    tripUpdates = []
    alerts = []
    time = []
    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)
	

        timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)
	
        for entity in feed.entity:
            # Trip update represents a change in timetable
            if entity.trip_update and entity.trip_update.trip.trip_id:
                update = tripupdate.tripupdate()
		
                ##### INSERT TRIPUPDATE CODE HERE ####	

                update.tripId       = entity.trip_update.trip.trip_id
                update.routeId      = entity.trip_update.trip.route_id
                update.startDate    = entity.trip_update.trip.start_date
                update.direction    = entity.trip_update.stop_time_update[-1].stop_id[-1]
                update.vehicleData  = entity.trip_update.vehicle
		newkey = entity.trip_update.stop_time_update[-1].stop_id
		low    = entity.trip_update.stop_time_update[-1].arrival.time
		hi     = entity.trip_update.stop_time_update[-1].departure.time
                update.futureStops.update({ newkey : [low,hi] })# Format {stopId : [arrivalTime,departureTime]})
            if entity.vehicle and entity.vehicle.trip.trip_id:
                v = vehicle.vehicle()
                
                ##### INSERT VEHICLE CODE HERE #####
                
                v.currentStopNumber = entity.vehicle.current_stop_sequence
                v.currentStopId     = entity.vehicle.stop_id
                v.timestamp         = entity.vehicle.timestamp
		v.currentStopStatus = entity.vehicle.current_status
	   	v.tripid	    = entity.vehicle.trip.trip_id	
            if entity.alert:
                a = alert.alert()
                
                #### INSERT ALERT CODE HERE #####
                #print entity.alert.informed_entity
                a.tripId            = entity.trip_update.trip.trip_id
                a.routeId           = entity.trip_update.trip.route_id
                a.startDate         = entity.trip_update.trip.start_date
		try:
                    a.alertMessage  = entity.alert.header_text.translation.text
		except:
		    a.alertMessage  = ''
	
		self.alerts.append(a)
		try:
			self.tripUpdates.append(v)
		except:
			pass
		try:
			self.tripUpdates.append(v)
		except:
			pass
		self.tripUpdates.append(timestamp)
	return self.tripUpdates


    # END OF getTripUpdates method
