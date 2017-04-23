import copy
# We only want certain fields in the gps_data. This is a way
# to explicitly specify what datafields we want.
GPS_FIELDS = ['mode',
              'time',
              'ept',
              'lat',
              'lon',
              'alt',
              'epx',
              'epy',
              'cpv',
              'track',
              'speed',
              'climb',
              'epd',
              'eps',
              'epc']

def scan_factory(gsm, gps_before, gps_after, sensor_name=None, high_quality=True):
    '''This takes python dictionaries with scan data and makes a Scan obj

    Args:
        gsm (Dict): Contains the Gsm_Scan Data
        gps_before (Dict): Contains the Gps_Scan Data
        gsm_after (Dict): Contains the Gps_Scan Data
        sensor_name (String): The identifier of the sensor that took
            the measurements.
    '''
    gpsb = Gps_Scan(gps_before)
    gpsa = Gps_Scan(gps_after)

    # Make a basic gsm_scan with the blob and freqency ranges
    gsm_scan = Gsm_Scan(gsm['scan_blob'])

    # Now set frequency range, error, and jammed
    gsm_scan.set_error(gsm['error'])
    gsm_scan.set_jammed(gsm['jammed'])
    
    # Set if they exist
    if 'freq_low' in gsm and 'freq_high' in gsm:
        gsm_scan.set_freq_range(gsm['freq_low'], gsm['freq_high'])

    if 'antenna' in gsm:
        gsm_scan.set_antenna(gsm['antenna'])

    # Now specify the measurement params
    for raw_meas in gsm['measurements']:
        # Make either a Bcch_Measurement or Gsm_Measurement
        if 'bcch' in raw_meas:
            meas = Bcch_Measurement(raw_meas['measurement_blob'])

            raw_bcch = raw_meas['bcch']

            # Now add all of the extra bcch fields
            meas.set_arfcn_lst(raw_bcch['arfcns'], raw_bcch['num_arfcn'])
            meas.set_channel_lst(raw_bcch['channels'], raw_bcch['num_channels'])

            bcch_cpy = copy.deepcopy(raw_bcch)

            # We want to remove the mandatory bcch fields
            del bcch_cpy['arfcns']
            del bcch_cpy['num_arfcn']
            del bcch_cpy['channels']
            del bcch_cpy['num_channels']

            meas.set_bcch_data(bcch_cpy)
        else:
            meas = Gsm_Measurement(raw_meas['measurement_blob'])

        # Specify the arfcn and rx_lev
        meas.set_arfcn(raw_meas['arfcn'])
        meas.set_rx_lev(raw_meas['rx_lev'])

        # Finally add in the measurement
        gsm_scan.add_measurement(meas)

    return Scan(gsm_scan, gpsb, gpsa, sensor_name, high_quality)

class Scan():
    '''This is the combination of 2 GPS points and a gsm mesurement'''
    def __init__(self, gsm, gpsb, gpsa, sensor_name=None, high_quality=True):
        self.gsm = gsm
        self.gps_before = gpsb
        self.gps_after = gpsa
        self.sensor_name = sensor_name
        self.high_quality = high_quality

    def document(self):
        """Return dictionary version of the Scan"""
        # Begin with the gsm docuement
        doc = {}
        doc['gsm'] = self.gsm.document()
        # Finally append the gps and gsm data
        doc['gps_before'] = self.gps_before.document()
        doc['gps_after'] = self.gps_after.document()

        doc['sensor_name'] = self.sensor_name
        doc['high_quality'] = self.high_quality

        return doc

    def get_gsm(self):
        return self.gsm

    def get_gps_before(self):
        return self.gps_before

    def get_gps_after(self):
        return self.gps_after

    def get_sensor_name(self):
        return self.sensor_name

    def get_high_quality(self):
        return self.high_quality

class Gps_Scan():
    '''This represents a single GPS point'''
    def __init__(self, raw_gps):
        # We need to remove the extra fields
        # This is the set of gps fields that exist in raw_gps dict
        cur_gps_fields = list(raw_gps.keys())

        # We want the intersection of fields that exist in the raw_gps dict and
        # that we are interested in storing (in GPS_FIELDS)
        fields = list(set(cur_gps_fields).intersection(GPS_FIELDS))

        self.gps_data = {gps_field: raw_gps[gps_field] for gps_field in fields}

    def get_gps_data(self):
        return self.gps_data

    def get_time(self):
        try:
            time = self.gps_data.get('time', None)
            if time is not None:
                return str(time)
            else:
                return None
        except:
            return None

    def get_mode(self):
        try:
            mode = self.gps_data.get('mode', None)
            if mode is not None:
                return str(mode)
            else:
                return None
        except:
            return None

    def document(self):
        ''' This formats the gps data so it can be objectified

        Return:
            (dict): The dictionary formatted version of the gps_data
        '''
        return self.gps_data

class Gsm_Scan():
    '''This object represents a gsm car scan.'''
    def __init__(self, blob, freq_low = None, freq_high = None):
        # If there is an error mentioned in the blob then this is set to 1
        self.blob = blob
        self.freq_low = freq_low
        self.freq_high = freq_high
        self.error = 0
        self.gsm_measurements = []
        self.jammed = 0

    def set_freq_range(self, freq_low, freq_high):
        self.freq_low = freq_low
        self.freq_high = freq_high

    def set_error(self, error):
        self.error = error

    def set_jammed(self, jammed):
        self.jammed = jammed

    def get_freq_range(self):
        return (self.freq_low, self.freq_high)

    def get_error(self):
        return self.error

    def get_jammed(self):
        return self.jammed

    def add_measurement(self, measurement):
        self.gsm_measurements.append(measurement)

    def measurement_cursor(self):
        for measurement in self.gsm_measurements:
            yield measurement

    # A nice printable string of the scan
    def __str__(self):
        s = ""
        s += "Error: " + str(self.error) + "\n\n"

        i = 0
        for i in range(len(self.gsm_measurements)):
            s += "===========================================\n"
            s += "Measurement " + str(i+1) + ":\n"
            s += "===========================================\n"
            s += str(self.gsm_measurements[i]) + "\n"
            i = i + 1

        return s

    def document(self):
        '''This makes a nice formated document that can be inserted to mongo'''
        doc = {}

        doc['scan_blob'] = self.blob
        if self.error == 0:
            doc['error'] = False
        else:
            doc['error'] = True

        if self.jammed == 1:
            doc['jammed'] = True
        else:
            doc['jammed'] = False

        # Set the frequency parameters if they exits
        if self.freq_low is not None:
            doc['freq_low'] = self.freq_low
        if self.freq_high is not None:
            doc['freq_high'] = self.freq_high

        measurements = []

        for measurement in self.gsm_measurements:
            measurements.append(measurement.document())

        doc['measurements'] = measurements

        return doc

class Gsm_Measurement():
    def __init__(self, gsm_blob):
        self.arfcn = None
        self.rx_lev = None
        self.blob = gsm_blob

    def set_arfcn(self, arfcn):
        self.arfcn = arfcn

    def set_rx_lev(self, rx_lev):
        self.rx_lev = rx_lev

    def get_arfcn(self):
        return self.arfcn

    def get_rx_lev(self):
        return self.rx_lev

    def __str__(self):
        s = ""
        s += "-------------RAW BEGIN-----------------\n"
        s += self.blob + "\n"
        s += "--------------RAW END-----------------\n"
        s += "arfcn: " + self.arfcn + "\n"
        s += "rx_lev: " + self.rx_lev + "\n"

        return s

    def document(self):
        '''This makes a nice formated document that can be inserted to mongo'''
        doc = {}

        doc['arfcn'] = int(self.arfcn)
        doc['rx_lev'] = int(self.rx_lev)
        doc['measurement_blob'] = self.blob

        return doc


class Bcch_Measurement(Gsm_Measurement):
    def __init__(self, gsm_blob):
        super().__init__(gsm_blob)

        # This is just a dictionary with all of the extra fields
        self.data = {}

        self.num_channels = None
        self.channels = []
        self.num_arfcn = None
        self.arfcns = []

    def set_arfcn_lst(self, arfcn_lst, num_arfcn):
        self.arfcns = arfcn_lst
        self.num_arfcn = num_arfcn

    def set_channel_lst(self, channel_lst, num_channels):
        self.channels = channel_lst
        self.num_channels = num_channels

    def set_bcch_data(self, data):
        self.data = data

    def get_arfcn_lst(self):
        return (self.arfcns, self.num_arfcn)

    def get_channel_lst(self):
        return (self.channels, self.num_channels)

    def get_data(self):
        return self.data

    def __str__(self):
        s = ""
        s += "-------------RAW BEGIN-----------------\n"
        s += self.blob + "\n"
        s += "--------------RAW END-----------------\n"
        s += "arfcn: " + self.arfcn + "\n"
        s += "rx_lev: " + self.rx_lev + "\n"
        s += "num_channels: " + self.num_channels + "\n"
        s += "channels: " + str(self.channels) + "\n"
        s += "num_arfcn: " + self.num_arfcn + "\n"
        s += "arfcns: " + str(self.arfcns) + "\n"

        for key in self.data:
            s += key + ": " + str(self.data[key]) + "\n"

        return s

    def document(self):
        '''This makes a nice formated document that can be inserted to mongo'''
        doc = {}

        # Add the mandatory fields
        doc['arfcn'] = int(self.arfcn)
        doc['rx_lev'] = int(self.rx_lev)
        doc['measurement_blob'] = self.blob

        # Add the bcch fields to this
        bcch = {}

        # Now the lists (also mandatory)
        bcch['num_channels'] = int(self.num_channels)
        bcch['num_arfcn'] = int(self.num_arfcn)

        # We have to convert the sub fields to the proper type
        format_channels = []
        format_arfcns = []
        for channel in self.channels:
            format_channels.append(int(channel))

        for arfcn in self.arfcns:
            format_arfcns.append(int(arfcn))

        bcch['channels'] = format_channels
        bcch['arfcns'] = format_arfcns

        # Finally add in the optional data fields
        for key in self.data:
            # All fields are ints except for a few
            # WARNING: If the fields change this is likely to break
            # since we are assuming that all of the fields that aren't
            # explicitly enumerated are ints

            # If the value should be a string or is already None then
            # just assign normally
            if self.data[key] is None:
                bcch[key] = self.data[key]
            elif key == 'cell_status':
                bcch[key] = self.data[key]
            elif key == 'ber':
                bcch[key] = float(self.data[key])
            # Everything else is an int
            else:
                bcch[key] = int(self.data[key])

        # Add the bcch dict to the doc
        doc['bcch'] = bcch

        return doc

