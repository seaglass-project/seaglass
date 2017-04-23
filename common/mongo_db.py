from pprint import pprint
import pymongo
import time
import sys
import traceback
import os
import base64
import datetime

import common.utils as utils
import common.scan as scan

DB_INSERT_TIMEOUT = 1
# Used to prevent timeouts on cursors
BATCH_SIZE = 1000
# Nice to have some versioning
VERSION = 0

class Database():
    ''' This is a helpful class to handle the necessary database operations'''

    def __init__(self, db_name, collection_name, host="localhost", port=27017, authentication=None):
        '''Establishes the database connection

        Args:
            db_name (String): Name of the database
            collection_name (String): Name of the collection
            host (String): hostname of database
            port (int): port of database
            authentication ({'username' : XXX, 'password' : XXX, 'source' : XXX}):
                This specifies the authentication parameters if necessary. If not specified
                then no authentication is used. All of these arguments must be present in
                the authenticaiton string.
        '''
        client = pymongo.MongoClient(host, port)

        if authentication is not None:
            # Raise an exception if some of the authentication params are missing
            if 'username' not in authentication or \
                    'password' not in authentication or \
                    'source' not in authentication:

                raise Exception("Missing critical authentication argument")

            # Now do the actual authentication
            client[db_name].authenticate(authentication['username'], \
                                        authentication['password'], \
                                        source=authentication['source'])

        self.collection = client[db_name][collection_name]

    def insert_sensor_point(self, full_scan, version=VERSION):
        ''' This will insert a scan point + gps into the database

        Args:
            scan (Scan): The object that represents the entire scan
        '''
        # Begin with the scan document
        mongo_dict = full_scan.document()

        rand = os.urandom(128)
        mongo_dict['unique_id'] = base64.b64encode(rand).decode('utf-8')
        mongo_dict['version'] = version

        self.insert_mongo_point(mongo_dict)

    def insert_mongo_point(self, mongo_dict):
        # If the connection has a timeout then just keep trying.
        # If the database is down there is no point in collecting
        # data anyway.
        insertion_successful = False
        while not insertion_successful:
            try:
                # Finally insert the point and set a bool to leave the loop
                utils.log("Trying to write to the DB...")
                self.collection.insert_one(mongo_dict)
                insertion_successful = True
                utils.log("Done writing to DB.")
            except Exception as e:
                exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
                traceback.print_exception(exceptionType, exceptionValue,
                                          exceptionTraceback, file=sys.stdout)
                utils.log("Error writing to DB: {}".format(e))
                time.sleep(DB_INSERT_TIMEOUT)

    def insert_mongo_points(self, mongo_dicts):
        # If the connection has a timeout then just keep trying.
        # If the database is down there is no point in collecting
        # data anyway.
        insertion_successful = False
        while not insertion_successful:
            try:
                # Finally insert the point and set a bool to leave the loop
                utils.log("Trying to write to the DB...")
                self.collection.insert_many(mongo_dicts)
                insertion_successful = True
                utils.log("Done writing to DB.")
            except Exception as e:
                exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
                traceback.print_exception(exceptionType, exceptionValue,
                                          exceptionTraceback, file=sys.stdout)
                utils.log("Error writing to DB: {}".format(e))
                time.sleep(DB_INSERT_TIMEOUT)

    def get_scans(self, uuids=None):
        '''This returns a iterable object to get all of the scan objects in the db'''

        # Just start grabbing all of the points
        points = self.collection.find()

        # The points object is a pymongo cursor. However there are timeouts if the
        # cursor reads too many points so we will set it manually
        points.batch_size(BATCH_SIZE)

        i = 0
        for point in points:
            i += 1
            if i % 1000 == 0:
                print("Collection point number: ", i)

            if uuids is None or point['unique_id'] not in uuids:
                gps_before = point['gps_before']
                gps_after = point['gps_after']
                gsm = point['gsm']
                sensor_name = point['sensor_name']
                uuid = point['unique_id']
                version = point['version']

                yield (scan.scan_factory(gsm, gps_before, gps_after, sensor_name), uuid, version)
