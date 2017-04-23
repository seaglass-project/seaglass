import re
import sys
from common.scan import Gsm_Scan, Gsm_Measurement, Bcch_Measurement

class Telit_Modem_Parser():
    def __init__(self):
        pass

    def parse_measurement(self, meas_blob):
        meas = None

        # There may be extra newlines stuck in there that we want to ignore
        cleaned_mb = meas_blob.replace("\r\n", "")

        # This is the pattern for the short measurments (non bcch)
        nonbcch_match = re.search(\
            r"""
            arfcn:                  
            [ ](?P<arfcn>\d+)[ ]    # arfcn data is just a positive num
            rxLev:                  
            [ ](?P<rx_lev>[\d|\-]+)[ ]?  #rxLev data is a negative number
            """, cleaned_mb, re.VERBOSE)

        # This is the pattern for the large bcch entries.
        bcch_match = re.search(\
            r"""
            arfcn:
            [ ](?P<arfcn>\d+)[ ]            # int (non-neg)
            bsic: 
            [ ](?P<bsic>\d+)[ ]             # int (non-neg)
            rxLev: 
            [ ](?P<rx_lev>[\d|\-]+)[ ]       # int (non-neg)
            ber: 
            [ ](?P<ber>[\d|.]+)[ ]          # float (non-neg)
            mcc:
            ([ ](?P<mcc>\d+)[ ] |           # int (non-neg)
            [ ]FFF[ ])                      # When the cellStatus is CELL_OTHER it sometimes will
                                            # make this a junk value (FFF)
            mnc:
            ([ ](?P<mnc>\d+)[ ] |             # int (non-neg)
            [ ]FF[ ])                        # When cellStatus is CELL_OTHER it can be a junk value
                                            # of FF
            lac:
            [ ](?P<lac>\d+)[ ]              # int (non-neg)
            cellId:
            [ ](?P<cell_id>\d+)[ ]           # int (non-neg)
            cellStatus:
            [ ](?P<cell_status>[A-Z|_]+)[ ]  # string (capatal letter)
            numArfcn:
            [ ](?P<num_arfcn>\d+)[ ]         # int (non-neg)
            arfcn:                          
            [ ]?((?P<arfcns>[ |\d+]+)[ ]?)*      # This is a sequence of space
                                            # separated ints (non-neg)
            (
              numChannels:
              [ ](?P<num_channels>\d+)[ ]      # int (non-neg)
              array:
              [ ]?((?P<array>[ |\d+]+)[ ]?)*
            )*                                 # This a sequence of space separated
                                            # ints (non-neg)
                                            # There seems to sometimes be a glitch where it
                                            # has been repeated twice.
                                            # Occasionally records end with either the channel
                                            # array or arfcn list. This creates problems with
                                            # the parsing, that is why the regex's for them are
                                            # so dirty.

            (
              pbcch:                         # Starting with pbcch - pcMeasCh if the cellStatus is CELL_OTHER
                                             # then these will no be present
              [ ](?P<pbcch>\d+)[ ]            # int (non-neg)
              nom:
              [ ](?P<nom>\d+)[ ]              # int (non-neg)
              rac:
              [ ](?P<rac>\d+)[ ]              # int (non-neg)
              spgc:
              [ ](?P<spgc>\d+)[ ]             # int (non-neg)
              pat:
              [ ](?P<pat>\d+)[ ]              # int (non-neg)
              nco:
              [ ](?P<nco>\d+)[ ]              # int (non-neg)
              t3168:
              [ ](?P<t3168>\d+)[ ]            # int (non-neg)
              t3192:
              [ ](?P<t3192>\d+)[ ]            # int (non-neg)
              drxmax:
              [ ](?P<drxmax>\d+)[ ]           # int (non-neg)
              ctrlAck:
              [ ](?P<ctrl_ack>\d+)[ ]          # int (non-neg)
              bsCVmax:
              [ ](?P<bscvmax>\d+)[ ]          # int (non-neg)
              alpha:
              [ ](?P<alpha>\d+)[ ]            # int (non-neg)
              pcMeasCh:
              [ ](?P<pc_meas_ch>\d+)[ ]{1,2}  # int (non-neg) -- sometimes there are
                                              # multiple spaces
            )?    
            (
              mstxpwr:
              [ ](?P<mstxpwr>\d+)[ ]          # int (non-neg)
              rxaccmin:
              [ ](?P<rxaccmin>\d+)[ ]         # int (non-neg)
              croffset:
              [ ](?P<croffset>\d+)[ ]         # int (non-neg)
              penaltyt:
              [ ](?P<penaltyt>\d+)[ ]         # int (non-neg)
              t3212:
              [ ](?P<t3212>\d+)[ ]{1,2}       # int (non-neg). Also, 2 spaces.
              CRH:
              [ ](?P<crh>\d+)[ ]?             # int (non-neg). Last entry may
                                              # have no spaces.
            )?
            """, cleaned_mb, re.VERBOSE)

        # We need to parse all the components of the bcch
        if bcch_match is not None:
            # The Bcch_Measurement obj we will be returning
            meas = Bcch_Measurement(meas_blob)
            # Making a hard copy of the dict because I am removing elements later
            bcch_data = dict(bcch_match.groupdict())

            # Checking that certain fields are always defined
            # This may not actually matter. I think these fields will always be defined
            # but their data will be None if there is no mattching element
            if 'arfcn' not in bcch_data or 'rx_lev' not in bcch_data \
                    or 'num_channels' not in bcch_data or 'num_arfcn' not in bcch_data \
                    or 'arfcns' not in bcch_data or 'array' not in bcch_data:
                    
                    print(meas_blob)
                    assert False, "Some critical BCCH fields are missing"

            # Grabbing some of the data out directly so it can be added to the
            # appropriate fields
            arfcn = bcch_data['arfcn']
            rx_lev = bcch_data['rx_lev']
            arfcns_raw = bcch_data["arfcns"]
            channels_raw = bcch_data["array"]
            nchannels = bcch_data['num_channels']
            narfcn = bcch_data['num_arfcn']


            # We want to remove the unnecessary fields so it can be properly
            # stored in the objects
            del bcch_data["arfcn"]
            del bcch_data["rx_lev"]
            del bcch_data["arfcns"]
            del bcch_data["array"]
            del bcch_data["num_channels"]
            del bcch_data["num_arfcn"]

            # Setting the measurement fields
            meas.arfcn = arfcn
            meas.rx_lev = rx_lev
            meas.num_arfcn = narfcn
            meas.num_channels = nchannels if nchannels else 0

            # This can happen if we have a blank array
            # so we have to check for this so stuff doesn't break
            if arfcns_raw is not None:
                arfcns_iter = re.finditer(\
                    r'\d+', arfcns_raw, re.VERBOSE)

                for arfcn_match in arfcns_iter:
                    arfcn = arfcn_match.group()
                    # Note that if there are duplicates this needs to have
                    # an additional check like in channels. However, there
                    # have been no duplicates so far.
                    meas.arfcns.append(arfcn)

            # This can happen if we have a blank array
            # so we have to check for this so stuff doesn't break
            if channels_raw is not None:
                channels_iter = re.finditer(\
                    r'\d+', channels_raw, re.VERBOSE)

                for channel_match in channels_iter:
                    channel = channel_match.group()
                    # There were some duplicate channels in some examples
                    # so we are going to test for it here
                    if channel not in meas.channels:
                        meas.channels.append(channel)

            # This is just a check that the number of arfcns and channels
            # match what would be expected.
            if int(meas.num_channels) != len(meas.channels):
                #or int(meas.num_arfcn) != len(meas.arfcns):

                print("Num channels: " + meas.num_channels)
                print("channels: " + str(meas.channels))
                print("Num arfcn: " + meas.num_arfcn)
                print("arfcn: " + str(meas.arfcn))

                print(cleaned_mb)
                assert False, "There is a discrepancy in the number of channels/arfcns" \
                                " that are expected and what has been parsed"

            # Finally update the data for the bcch object
            meas.data = bcch_data

        elif nonbcch_match is not None:
            meas = Gsm_Measurement(meas_blob)
            nonbcch_data = dict(nonbcch_match.groupdict())

            # Checking that certain fields are always defined
            if 'arfcn' not in nonbcch_data or 'rx_lev' not in nonbcch_data:
                print(meas_blob)
                assert False, "There is neither a arfc or rx_lev in a" \
                                " non-bcch entry"

            # Setting the measurement fields
            meas.arfcn = nonbcch_data['arfcn']
            meas.rx_lev = nonbcch_data['rx_lev']

        elif nonbcch_match is None and bcch_match is None:
            print(cleaned_mb)
            assert False, "The parsed measurement was neither a bcch or" \
                            " a non-bcch measurment"
        else:
            print(cleaned_mb)
            assert False, "The parsed measurement was both a bcch and" \
                            " a non-bcch measurement"

        return meas

    '''
    This function takes a full raw gsm scan blob and then parses the data
    into reasonable datastructures. It does this using the python regex lib.
    '''
    def parse_scan(self, scan_blob):
        scan = Gsm_Scan(scan_blob)

        # First we want to see if the scan has an ERROR or OK (at least one
        # of those should be present)
        is_error = re.search("ERROR", scan_blob)
        is_ok = re.search("OK", scan_blob)
        is_jammed = re.search("JAMMED", scan_blob)
        
        # This is just a sanity check
        if is_error and is_ok:
            print(scan_blob)
            sys.stdout.flush()
            assert False, "There is both an OK and ERROR"

        # Set the error bit
        if not is_error and not is_ok:
            scan.error = 2
            print("scan error 2")
            return scan
        elif is_error:
            scan.error = 1
        else:
            scan.error = 0

        # Detect if there is a jam
        if is_jammed:
            scan.jammed = 1
        else:
            scan.jammed = 0

        # This is the regex that defines all of the measurements in the scan.
        # this pattern object can be used to do matches on the gsm blob
        meas_pattern = re.compile(\
            r"""
            arfcn               # All valid data points start with arfcn.
                                # If for some reason this strattles more than one line
                                # then we will ignore the measurment.

            (                   # One or more lines per match

              (
                (?!ERROR)       # This negative look ahead assertion should check that
                                # an error doesn't occur in the middle of a measurement.
                                # If it does then we throw the meas out.

                (\w|\ |:|\-|\.) # This should pick up any of the usual characters
                                # in a measurement

              )+                # go through as many characters as possible in the meas

              \r\n              # Each line ends with /r/n
            )+                  

            (\r\n){2}           # This separates all measurements
            """, re.VERBOSE | re.DOTALL)

        # Iterate over all of the matches
        meas_iter = meas_pattern.finditer(scan_blob)

        for match in meas_iter:
            meas = self.parse_measurement(match.group())
            scan.gsm_measurements.append(meas)

        return scan
