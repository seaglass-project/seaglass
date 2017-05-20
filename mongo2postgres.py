#!/usr/bin/env python3

from pprint import pprint
import time
import sys
import os

import postgres_config
import common.utils as utils
import common.mongo_db as mongo_db
import common.postgres_db as postgres_db
import common.scan as scan

INSERT_NUM = 100

def main():
    mdb = mongo_db.Database("SensorDB", "Scan")
    
    pdb = postgres_db.Database(postgres_config.database, \
                                postgres_config.username, \
                                postgres_config.password, \
                                postgres_config.hostname, \
                                postgres_config.port)

    # Purging tables
    # Uncomment this if you want to delete all old data
    # in the tables (the tables must already be defined)
    #utils.log("Purging tables...")
    #pdb.purge_tables()

    # Go ahead and initialize the tables. No harm if they are
    # already initialized.
    utils.log("Initializing tables...")
    pdb.init_tables()

    # Grab the uuids that exist
    uuids = pdb.get_uuids()

    # Read in each of the scan objects
    scan_uuid_lst = []
    i = 0
    for (full_scan, uuid, version) in mdb.get_scans(uuids=uuids):
        i = i + 1
        if i % 100 == 0:
            pprint("Scan number: {:d}".format(i))

        # Actually append the scan to the full scan list
        scan_uuid_lst.append((full_scan, uuid, version,))

        if len(scan_uuid_lst) >= INSERT_NUM:
            pdb.insert_scans(scan_uuid_lst)
            scan_uuid_lst = []

    if len(scan_uuid_lst) > 0:
        pdb.insert_scans(scan_uuid_lst)

if __name__ == '__main__':
    main()
