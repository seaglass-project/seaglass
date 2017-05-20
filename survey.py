#!/usr/bin/env python3
import traceback
import sys
import time

import sensor.gps as gps
import sensor.gsm as gsm
import common.mongo_db as db
import common.utils as utils

from common.scan import Gps_Scan, Gsm_Scan, Scan
from common.parse import Telit_Modem_Parser

SCAN_PAUSE = 1              # How long of a pause between scans (in sec)
DB_NAME = "SensorDB"
COLLECTION_NAME = "Scan"

# This will initialize the tables if needed
def initialize(modem_tty):
    '''This initializes all of the objects that are necessary.

    This includes: 1) Database, 2) gps objects, and 3) gsm objs

    Return:
        (Database, GpsScanner, GsmScanner): This tuple contains the database
        connection and both of the scan objects that will be used to run a scan.
    '''
    # Initialize the GpsScanner
    gps_scanner = gps.GpsScanner()
    # Initialize the GsmScanner
    gsm_scanner = gsm.GsmScanner(modem_tty)
    # Sets up the database connection
    # This will look for a connection locally with no
    # authentication
    database = db.Database(DB_NAME, COLLECTION_NAME)

    return (database, gps_scanner, gsm_scanner)

def scan(database, gps_scanner, gsm_scanner):
    '''This runs one iteration of a scan.

    Currently, one scan iteration is a gps scan followed by a
    gsm scan followed by a gps scan. Then it inserts this data
    into the database.

    Args:
        database (Database) : The database connection object to do insertions
        gps_scanner (GpsScanner): This is an object that manages the gps
            connection.
        gsm_scanner (GsmScanner): This is the object that manages
            the gsm connection.
    
    '''
    # Get data from GPS and modem
    utils.log("Collecting GPS and modem data...")

    gps_before = Gps_Scan(gps_scanner.scan())
    utils.log_gps_time(gps_before.get_time(), gps_before.get_mode())

    # Grap the gsm scan data and then parse it into as Scan object
    raw_gsm_data = gsm_scanner.scan()
    
    # Create a parser and parse the blob to make a Gsm_Scan
    parser = Telit_Modem_Parser()
    gsm_scan = parser.parse_scan(raw_gsm_data['data_blob'])
    # Now add the frequency range to the gsm_scan obj
    gsm_scan.set_freq_range(raw_gsm_data['freq_low'], raw_gsm_data['freq_high'])

    gps_after = Gps_Scan(gps_scanner.scan())

    # Now gather the data into a scan object
    scan = Scan(gsm_scan, gps_before, gps_after, utils.get_sensor_name())
    utils.log("Done collecting GPS and modem data.")

    # Actually insert the database points
    database.insert_sensor_point(scan)

def scan_loop(modem_tty):
    '''This endlessly loops taking gps and gsm scans and writing them to a db
    
    This function never terminates until the program stops or
    there is an error.
    '''
    # This will create tables if needed
    (database, gps_scanner, gsm_scanner) = initialize(modem_tty)

    i = 0
    while True:
        i = i + 1

        # Perform the scan
        utils.log("Begin Scan: {:d}".format(i))

        try:
            scan(database, gps_scanner, gsm_scanner)
        except Exception as e:
            utils.log("Exception in Scan...")
            utils.log(str(e))

            # Grab the exception and then print it.
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback, file=sys.stdout)

            # It is good to close the modem so that it should work right when the program is run.
            utils.log("Closing modem...")
            gsm_scanner.close()
            utils.log("Closed modem.")

            utils.log("End Scan: {:d}".format(i))

            sys.exit(-1)

        # Sleep between scans
        time.sleep(SCAN_PAUSE)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        utils.log("Usage: ./survey <modem_tty>")
        sys.exit(-1)

    utils.log("#########################")
    utils.log("Beginning cellular survey.")
    utils.log("#########################")
    
    modem_tty = sys.argv[1]

    scan_loop(modem_tty)
