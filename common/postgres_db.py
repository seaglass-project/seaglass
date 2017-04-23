import time
import psycopg2

from common.scan import Gsm_Scan, Gps_Scan, Scan, Gsm_Measurement, Bcch_Measurement
from common.parse import Telit_Modem_Parser
import common.utils as utils

# These are string constants that define the database schema
SCAN_SCHEMA = '''
                id bigserial primary key,
                gsm_id bigint references Gsm_Scan(id),
                gps_before_id bigint references Gps_Scan(id),
                gps_after_id bigint references Gps_Scan(id),
                sensor_name text,
                uuid text unique,
                version Integer,
                high_quality boolean
                '''

GSM_SCAN_SCHEMA = '''
                    id bigserial Primary Key,
                    freq_low Integer,
                    freq_high Integer,
                    error boolean,
                    jammed boolean
                    '''

GPS_SCAN_SCHEMA = '''
                    id bigserial primary key,
                    mode Integer,
                    time timestamp,
                    ept double precision,
                    lat double precision,
                    lon double precision,
                    alt double precision,
                    epx double precision,
                    epy double precision,
                    epv double precision,
                    track double precision,
                    speed double precision,
                    climb double precision,
                    epd double precision,
                    eps double precision,
                    epc double precision
                    '''

GSM_MEASUREMENT_SCHEMA = '''
                    id bigserial Primary Key,
                    gsm_scan_id bigint references Gsm_Scan(id),
                    arfcn Integer,
                    rx_lev Integer,
                    tmp_id bigint UNIQUE
                    '''

BCCH_MEASUREMENT_SCHEMA = '''
                    id bigserial Primary Key,
                    gsm_measurement_id bigint references Gsm_Measurement(id),
                    bsic Integer,
                    ber double precision,
                    mcc Integer,
                    mnc Integer,
                    lac Integer,
                    cell_id Integer,
                    cell_status Text,
                    num_arfcn Integer,
                    num_channels Integer,
                    pbcch Integer,
                    nom Integer,
                    rac Integer,
                    spgc Integer,
                    pat Integer,
                    nco Integer,
                    t3168 Integer,
                    t3192 Integer,
                    drxmax Integer,
                    ctrl_ack Integer,
                    bscvmax Integer,
                    alpha Integer,
                    pc_meas_ch Integer,
                    mstxpwr Integer,
                    rxaccmin Integer,
                    croffset Integer,
                    penaltyt Integer,
                    t3212 Integer,
                    crh Integer,
                    tmp_id bigint UNIQUE
                    '''

CHANNEL_LIST_SCHEMA = '''
                    id bigserial Primary Key,
                    bcch_measurement_id bigint references Bcch_Measurement(id),
                    channel Integer
                    '''

ARFCN_LIST_SCHEMA = '''
                    id bigserial Primary Key,
                    bcch_measurement_id bigint references Bcch_Measurement(id),
                    arfcn Integer
                    '''

# The following are insertion statements for each of the tables (with wildcards)
SCAN_INSERT = '''
                Insert INTO Scan(gsm_id, gps_before_id, gps_after_id, sensor_name,
                                uuid, version, high_quality)
                VALUES(%s,%s,%s,%s,%s,%s,%s);
              '''

GSM_SCAN_INSERT = '''
                         Insert INTO Gsm_Scan(freq_low, freq_high, error, jammed)
                         VALUES(%s,%s,%s,%s)
                         RETURNING id;
                        '''

GPS_SCAN_INSERT = '''
                         Insert INTO Gps_Scan(mode,time,ept,lat,lon,alt,
                                                        epx,epy,epv,track,speed,
                                                        climb,epd,eps,epc)
                         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                         RETURNING id;
                        '''

GSM_MEASUREMENT_INSERT = '''
                            Insert INTO GSM_Measurement(gsm_scan_id, arfcn, rx_lev, tmp_id)
                            VALUES
                         '''
BCCH_MEASUREMENT_INSERT = '''
                            Insert INTO Bcch_Measurement(gsm_measurement_id, bsic, ber,
                                                         mcc, mnc, lac, cell_id, cell_status,
                                                         num_arfcn, num_channels, pbcch, nom,
                                                         rac, spgc, pat, nco, t3168, t3192,
                                                         drxmax, ctrl_ack, bscvmax, alpha,
                                                         pc_meas_ch, mstxpwr, rxaccmin,
                                                         croffset, penaltyt, t3212, crh, tmp_id)
                            VALUES
                          '''

ARFCN_LIST_INSERT = '''Insert INTO Arfcn_List(bcch_measurement_id, arfcn)
                    VALUES
                    '''
CHANNEL_LIST_INSERT = '''Insert INTO Channel_List(bcch_measurement_id, channel)
                        VALUES
                        '''

class Database():
    def __init__(self, dbname, user, password, host, port):
        '''Creates the database object and initializes the connection

        Args:
            db_path (String): A path to the sqlite database file
        '''
        self.con = psycopg2.connect(dbname=dbname, user=user, \
                                    password=password, host=host, port=port)

    def purge_tables(self):
        cur = self.con.cursor()

        # The order matters because of foreign keys
        cur.execute(self.drop_table_cmd("Arfcn_List"))
        cur.execute(self.drop_table_cmd("Channel_List"))
        cur.execute(self.drop_table_cmd("Bcch_Measurement"))
        cur.execute(self.drop_table_cmd("Gsm_Measurement"))
        cur.execute(self.drop_table_cmd("Scan"))
        cur.execute(self.drop_table_cmd("Gps_Scan"))
        cur.execute(self.drop_table_cmd("Gsm_Scan"))

        self.con.commit()

    def create_indices(self):
        cur = self.con.cursor()

        scan_version = '''Create index on scan (version);'''
        cur.execute(scan_version)

        tuple_index = '''Create index on bcch_measurement (mcc, mnc, lac, bsic, cell_Id);'''
        cur.execute(tuple_index)

        time_index = '''Create index on gps_scan (time);'''
        cur.execute(time_index)

        mode_index = '''Create index on gps_scan (mode);'''
        cur.execute(mode_index)

        lat_index = '''Create index on gps_scan (lat);'''
        cur.execute(lat_index)

        lon_index = '''Create index on gps_scan (lon);'''
        cur.execute(lon_index)

        earth_ll_index = '''CREATE INDEX gps_ll_to_earth_idx on gps_scan USING gist(ll_to_earth(lat, lon));'''
        cur.execute(earth_ll_index)

        point_index = '''CREATE INDEX point_idx on gps_scan USING gist(point(lat, lon));'''
        cur.execute(earth_ll_index)

        self.con.commit()

    def init_tables(self):
        cur = self.con.cursor()

        # The order matters because of the foreign keys
        cur.execute(self.create_table_cmd("Gsm_Scan", GSM_SCAN_SCHEMA))
        cur.execute(self.create_table_cmd("Gps_Scan", GPS_SCAN_SCHEMA))
        cur.execute(self.create_table_cmd("Scan", SCAN_SCHEMA))
        cur.execute(self.create_table_cmd("Gsm_Measurement", GSM_MEASUREMENT_SCHEMA))
        cur.execute(self.create_table_cmd("Bcch_Measurement", BCCH_MEASUREMENT_SCHEMA))
        cur.execute(self.create_table_cmd("Channel_List", CHANNEL_LIST_SCHEMA))
        cur.execute(self.create_table_cmd("Arfcn_List", ARFCN_LIST_SCHEMA))

        self.con.commit()

    # We have created temporary columns for the insertion and this is to remove them
    def clean_tables(self):
        cur = self.con.cursor()

        cur.execute(self.drop_col_cmd("Gsm_Measurement", "tmp_id"))
        cur.execute(self.drop_col_cmd("Bcch_Measurement", "tmp_id"))

        self.con.commit()

    def get_tmp_ids(self):
        cur = self.con.cursor()

        # If there is nothing in the database use 0
        gsm_measurement_tmp_id = 0
        bcch_measurement_tmp_id = 0

        # Just run the queries to get the largest tmp_ids
        # for both the Gsm_Measurement and Bcch_Measurement
        cur.execute('''Select max(GM.tmp_id)
                        From Gsm_Measurement GM
                    ''')

        gsm_measurement_fetch = cur.fetchone()[0]

        cur.execute('''Select max(BM.tmp_id)
                        From Bcch_Measurement BM
                    ''')

        bcch_measurement_fetch = cur.fetchone()[0]

        # Finally set the values and return them. The fetch will be None if no
        # results were returned (so there are no rows)
        # We add one because we want one larger than the max tmp_id
        gsm_measurement_tmp_id = (gsm_measurement_fetch + 1) \
                                        if gsm_measurement_fetch is not None else 0
        bcch_measurement_tmp_id = (bcch_measurement_fetch + 1) \
                                        if bcch_measurement_fetch is not None else 0

        return (gsm_measurement_tmp_id, bcch_measurement_tmp_id)

    def get_uuids(self):
        '''Return all of the uuids of the Scans as a list'''
        cur = self.con.cursor()

        cur.execute('''Select S.uuid
                        From Scan S
                    ''')

        uuids = []
        # If there are no records then this will just return []
        for row in cur.fetchall():
            uuids.append(row[0])

        return uuids

    def insert_scans(self, scan_uuids):
        '''This parses each of the scans and inserts them into the db'''
        utils.log("Inserting Scans into the database...")

        (gsm_measurement_tmp_id, bcch_measurement_tmp_id) = self.get_tmp_ids()

        cur = self.con.cursor()

        for (scan, uuid, version) in scan_uuids:
            # Create a gsm_scan_row
            gsm_scan = scan.get_gsm()
            freq_range = gsm_scan.get_freq_range()
            # freq_low, freq_high, error, jammed
            gsm_scan_row = (freq_range[0], freq_range[1], \
                            gsm_scan.get_error(), gsm_scan.get_jammed(),)

            # Create the before and after gps_scan_row
            gps_before = scan.get_gps_before()
            gps_after = scan.get_gps_after()

            def generate_gps_tuple(gps_data):
                '''Use this function to generate the gps tuples from the data dict'''
                return  (       gps_data.get('mode', None),
                                gps_data.get('time', None),
                                gps_data.get('ept', None),
                                gps_data.get('lat', None),
                                gps_data.get('lon', None),
                                gps_data.get('alt', None),
                                gps_data.get('epx', None),
                                gps_data.get('epy', None),
                                gps_data.get('epv', None),
                                gps_data.get('track', None),
                                gps_data.get('speed', None),
                                gps_data.get('climb', None),
                                gps_data.get('epd', None),
                                gps_data.get('eps', None),
                                gps_data.get('edc', None),
                        )

            gps_before_row = generate_gps_tuple(gps_before.get_gps_data())
            gps_after_row = generate_gps_tuple(gps_after.get_gps_data())

            # Now insert the gsm_scan, gps_before_scan, and gps_after_scan
            cur.execute(GSM_SCAN_INSERT, gsm_scan_row)
            gsm_scan_id = cur.fetchone()[0]

            cur.execute(GPS_SCAN_INSERT, gps_before_row)
            gps_before_id = cur.fetchone()[0]

            cur.execute(GPS_SCAN_INSERT, gps_after_row)
            gps_after_id = cur.fetchone()[0]

            # Using the ids we just collected, insert the scan
            scan_row = (gsm_scan_id, gps_before_id, gps_after_id,\
                                scan.get_sensor_name(),uuid,version,scan.get_high_quality(),)
            cur.execute(SCAN_INSERT, scan_row)

            # This is to enable ids on bluk insertion on bcch_measurements and gsm_measurements
            gsm_measurement_rows = []

            # This is a bit of a hack but we are inserting
            # {'row' : bcch_measurement_row, 'gsm_measurement_tmp_id' : gsm_measurement_tmp_id}
            # so that we can know what to reference
            bcch_measurement_rowlist_dicts = []

            # Finally we need to store the arfcn list and bcch lists
            arfcn_list_rowlist_dicts = []
            channel_list_rowlist_dicts = []

            # Used as a place holder for the foreign key
            tmp_id = 0

            # We need to know the range of gsm_measurement_ids to query for the inserted
            # points later
            gsm_measurement_tmp_id_low = gsm_measurement_tmp_id
            bcch_measurement_tmp_id_low = bcch_measurement_tmp_id

            for meas in gsm_scan.measurement_cursor():
                gsm_measurement_row = (gsm_scan_id, meas.get_arfcn(), \
                                        meas.get_rx_lev(), gsm_measurement_tmp_id)
                # Update the gsm_measurement_row
                gsm_measurement_rows.append(gsm_measurement_row)

                # If it is a bcch_measurement then insert those fields
                if isinstance(meas, Bcch_Measurement):
                    (arfcns, num_arfcn) = meas.get_arfcn_lst()
                    (channels, num_channels) = meas.get_channel_lst()
                    bcch_data = meas.get_data()

                    # Sadly this must be a list because tuples are immutable
                    # Thus it has the name bcch_measurement_rowlist
                    bcch_measurement_rowlist = [tmp_id,
                                            (bcch_data['bsic'] if bcch_data['bsic']\
                                                    is not None else "NULL"),
                                            (bcch_data['ber'] if bcch_data['ber']\
                                                    is not None else "NULL"),
                                            (bcch_data['mcc'] if bcch_data['mcc']\
                                                    is not None else "NULL"),
                                            (bcch_data['mnc'] if bcch_data['mnc']\
                                                    is not None else "NULL"),
                                            (bcch_data['lac'] if bcch_data['lac']\
                                                    is not None else "NULL"),
                                            (bcch_data['cell_id'] if bcch_data['cell_id']\
                                                    is not None else "NULL"),
                                            (bcch_data['cell_status'] if bcch_data['cell_status'] \
                                                    is not None else "NULL"),
                                            num_arfcn,
                                            num_channels,
                                            (bcch_data['pbcch'] if bcch_data['pbcch']\
                                                    is not None else "NULL"),
                                            (bcch_data['nom'] if bcch_data['nom']\
                                                    is not None else "NULL"),
                                            (bcch_data['rac'] if bcch_data['rac']\
                                                    is not None else "NULL"),
                                            (bcch_data['spgc'] if bcch_data['spgc']\
                                                    is not None else "NULL"),
                                            (bcch_data['pat'] if bcch_data['pat']\
                                                    is not None else "NULL"),
                                            (bcch_data['nco'] if bcch_data['nco']\
                                                    is not None else "NULL"),
                                            (bcch_data['t3168'] if bcch_data['t3168']\
                                                    is not None else "NULL"),
                                            (bcch_data['t3192'] if bcch_data['t3192']\
                                                    is not None else "NULL"),
                                            (bcch_data['drxmax'] if  bcch_data['drxmax']\
                                                    is not None else "NULL"),
                                            (bcch_data['ctrl_ack'] if bcch_data['ctrl_ack']\
                                                    is not None else "NULL"),
                                            (bcch_data['bscvmax'] if bcch_data['bscvmax']\
                                                    is not None else "NULL"),
                                            (bcch_data['alpha'] if bcch_data['alpha']\
                                                    is not None else "NULL"),
                                            (bcch_data['pc_meas_ch'] if  bcch_data['pc_meas_ch']\
                                                    is not None else "NULL"),
                                            (bcch_data['mstxpwr'] if bcch_data['mstxpwr']\
                                                    is not None else "NULL"),
                                            (bcch_data['rxaccmin'] if bcch_data['rxaccmin']\
                                                    is not None else "NULL"),
                                            (bcch_data['croffset'] if bcch_data['croffset']\
                                                    is not None else "NULL"),
                                            (bcch_data['penaltyt'] if bcch_data['penaltyt']\
                                                    is not None else "NULL"),
                                            (bcch_data['t3212'] if bcch_data['t3212']\
                                                    is not None else "NULL"),
                                            (bcch_data['crh'] if bcch_data['crh']\
                                                    is not None else "NULL"),
                                                    bcch_measurement_tmp_id
                                            ]

                    # We want to add the bcch_measurement_rowlist along side the
                    # gsm_measurement_tmp_id so it is easly to look it up
                    bcch_measurement_rowlist_dicts.append(\
                                        {'rowlist' : bcch_measurement_rowlist,
                                         'gsm_measurement_tmp_id' : gsm_measurement_tmp_id})


                    # Finally insert the arfcns and channels
                    for arfcn in arfcns:
                        arfcn_list_rowlist = [tmp_id, arfcn]
                        arfcn_list_rowlist_dicts.append(\
                                {'rowlist' : arfcn_list_rowlist,
                                 'bcch_measurement_tmp_id': bcch_measurement_tmp_id})

                    for channel in channels:
                        channel_list_rowlist = [tmp_id, channel]
                        channel_list_rowlist_dicts.append(\
                                {'rowlist' : channel_list_rowlist,
                                 'bcch_measurement_tmp_id': bcch_measurement_tmp_id})

                    # Now update the bcch_measurement_tmp_id
                    bcch_measurement_tmp_id += 1

                # Now update the gsm_measurement_tmp_id
                gsm_measurement_tmp_id += 1

            # Finally do the bulk inserts if there were measurements
            if len(gsm_measurement_rows) > 0:
                # First do an insert with the gsm_measurements
                gsm_measurement_query = GSM_MEASUREMENT_INSERT
                gsm_measurement_query += ','.join([str(g) for g in gsm_measurement_rows])
                gsm_measurement_query += ";"

                cur.execute(gsm_measurement_query)

                # Only if there is a bcch_measurement do we need to insert
                # into the bcch_measurement table
                if len(bcch_measurement_rowlist_dicts) > 0:
                    # Grab the id associated with each gsm_measurement
                    # Note that the gms_meaurement_tmp_id is one greater than
                    # the value we are searching for.
                    # Note: index 0: id and index 1: tmp_id
                    cur.execute('''Select GM.id, GM.tmp_id
                                    FROM Gsm_Measurement GM
                                    WHERE GM.tmp_id >= %s and
                                        GM.tmp_id < %s
                                ''', (gsm_measurement_tmp_id_low, gsm_measurement_tmp_id,))

                    # This is a dict with {tmp_id : real_id}
                    gsm_measurement_id_lookup = {}
                    for ids in cur.fetchall():
                        gsm_measurement_id_lookup[ids[1]] = ids[0]

                    # Now actually update the bcch_measurement foreign_key rows
                    bcch_measurement_rows = []
                    for bcch_meas_dict in bcch_measurement_rowlist_dicts:
                        # Grab the real id
                        tmp_id = bcch_meas_dict['gsm_measurement_tmp_id']
                        real_id = gsm_measurement_id_lookup[tmp_id]

                        # Now grab the row and update the first entry to the real id
                        bcch_meas_rowlist = bcch_meas_dict['rowlist']
                        bcch_meas_rowlist[0] = real_id
                        bcch_measurement_rows.append(tuple(bcch_meas_rowlist))

                    bcch_measurement_query = BCCH_MEASUREMENT_INSERT
                    bcch_measurement_query += ','.join([str(b) for b in bcch_measurement_rows])
                    bcch_measurement_query += ";"
                    # We need to replace "NULL" with NULL in the query string so we don't insert
                    # the string "NULL" instead of an actual NULL
                    nullified_bcch_measurement_query = bcch_measurement_query.replace("'NULL'", "NULL")
                    cur.execute(nullified_bcch_measurement_query)

                    # We need to build our index lookup for the bcch measurements
                    if len(arfcn_list_rowlist_dicts) > 0 or \
                            len(channel_list_rowlist_dicts) > 0:

                        cur.execute('''SELECT BM.id, BM.tmp_id
                                        FROM Bcch_Measurement BM
                                        WHERE BM.tmp_id >= %s and
                                            BM.tmp_id < %s
                                    ''', (bcch_measurement_tmp_id_low,bcch_measurement_tmp_id,))

                        bcch_measurement_id_lookup = {}
                        for ids in cur.fetchall():
                            bcch_measurement_id_lookup[ids[1]] = ids[0]

                        if len(arfcn_list_rowlist_dicts) > 0:
                            arfcn_list_rows = []
                            for arfcn_list_dict in arfcn_list_rowlist_dicts:
                                tmp_id = arfcn_list_dict['bcch_measurement_tmp_id']
                                real_id = bcch_measurement_id_lookup[tmp_id]

                                arfcn_list_rowlist = arfcn_list_dict['rowlist']
                                arfcn_list_rowlist[0] = real_id
                                arfcn_list_rows.append(tuple(arfcn_list_rowlist))

                            arfcn_list_query = ARFCN_LIST_INSERT
                            arfcn_list_query += ','.join([str(a) for a in arfcn_list_rows])
                            arfcn_list_query += ';'

                            cur.execute(arfcn_list_query)

                        if len(channel_list_rowlist_dicts) > 0:
                            channel_list_rows = []
                            for channel_list_dict in channel_list_rowlist_dicts:
                                tmp_id = channel_list_dict['bcch_measurement_tmp_id']
                                real_id = bcch_measurement_id_lookup[tmp_id]

                                channel_list_rowlist = channel_list_dict['rowlist']
                                channel_list_rowlist[0] = real_id
                                channel_list_rows.append(tuple(channel_list_rowlist))

                            channel_list_query = CHANNEL_LIST_INSERT
                            channel_list_query += ','.join([str(c) for c in channel_list_rows])
                            channel_list_query += ';'

                            cur.execute(channel_list_query)

            # We want to commit the transaction on every scan that we insert.
            # Each scan should be inserted as an atomic operation.
            self.con.commit()

        utils.log("Done inserting Scans...")

    def create_table_cmd(self, tablename, schema_str):
        s = "CREATE TABLE if not EXISTS"
        s += " " + tablename + "\n"
        s += "(" + schema_str + ");"

        return s

    def drop_table_cmd(self, tablename):
        return "DROP TABLE " + tablename + " CASCADE ;"

    def drop_col_cmd(self, tablename, colname):
        return "ALTER TABLE IF EXISTS" + tablename + " DROP COLUMN " + colname + ";"
