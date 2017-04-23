import serial
import common.utils as utils
import time

MODEM_BAUD = 115200
MODEM_TIMEOUT = 1
MODEM_READ_PAUSE = 0.5
MODEM_TIMELIMIT = 120

# This is a set of interesting telit AT-commands
AT_COMMANDS = {'surv' : 'at#csurv',
            'surv_channel_range' : 'at#csurv={:d},{:d}', 
            'surv_num' : 'at#csurvb={:d}',
            'nsurv' : 'at#csurvc',
            'nsurv_channel' : 'at#csurvuc={:d}',
            'nsurv_num' : 'at#csurvbc={:d}',
            'surv_setting' : 'at#csurvext={:d}',
            'jam_setting' : 'at#jdr={:d}',
            'error_report': 'at+ceer' }

# We break the full spectrum scan into chunks of channels to speed up each
# individual scan. That way the GPS coordinates of the measurement
# will be more accurate. This is probably not the ideal way to split,
# but it seems to work well enough.
FREQUENCY_SPLIT =   [ (0, 127),
                      (128, 175),
                      (176, 181),
                      (182, 232),
                      (233, 238),
                      (239, 251),
                      (252, 511),
                      (512, 737),
                      (738, 744),
                      (745, 752),
                      (753, 885),
                      (886, 1023)
                    ]

# This class is required to pull data from the modem
class GsmScanner():

    def __init__(self, modem_tty):
        # initialize the modem object that will be used to communicate
        # with the modem
        self.modem = serial.Serial(modem_tty, MODEM_BAUD, timeout=MODEM_TIMEOUT)

        # Use this to keep track of the current split that we are measuring
        self.split_index = 0

        # Put the mode in an extended scan mode (number 3)
        res = self.run_at_command('surv_setting', 3)

        # Enable jamming detection
        res = self.run_at_command('jam_setting', 2)

    def close(self):
        self.modem.close()

    def scan(self):
        modem_data = {}

        # Wrap around the split index if necessary (mod would also work)
        if self.split_index >= len(FREQUENCY_SPLIT):
            self.split_index = 0

        freq_split = FREQUENCY_SPLIT[self.split_index]

        utils.log("Reading modem scan data on arfcn range: (" + str(freq_split[0]) + \
                                                    ", " + str(freq_split[1]) + ")")

        # Grab the data blob from the modem
        modem_data['data_blob'] = self.run_at_command('surv_channel_range', \
                                        freq_split[0] , freq_split[1])
        # Now record the last split that was used
        modem_data['freq_low'] = freq_split[0]
        modem_data['freq_high'] = freq_split[1]

        # Update to the next split
        self.split_index = self.split_index + 1

        utils.log("Done reading modem scan data.")

        return modem_data

    def run_at_command(self, command, command_arg1=None, command_arg2=None):

        # Format and then write the at command
        at_cmd = self.format_at_command(command, command_arg1, command_arg2)
        self.modem.write(at_cmd)

        res = ""
        modem_time = 0

        # This loop will run until either the modem timelimit has expired or
        # we have seen an 'OK' or 'ERROR' in the message from the modem
        while modem_time < MODEM_TIMELIMIT:
            waitbytes = self.modem.inWaiting()
            # The modem returns bytes so we need to make it a str
            in_bytes = self.modem.read(waitbytes)

            # Occassionally the data returned back from the modem is not
            # decodable. We just log the error and continue.
            try:
                res += in_bytes.decode('UTF-8')
            except Exception as e:
                # The data we are looking at is probably bad. In this case
                # it makes sense to clear the modem read buffer and just
                # log what we see. Then break. Hopefully doing this for
                # MODEM_TIMELIMIT will be enough to clear the bad data
                # from the modem. 
    
                # First we want to print whatever we have seen if anything
                # in the res string (this should be good data)
                utils.log("Crash in Decoding!")

                utils.log("Clearing Bad Modem Data...")
                # Clear the buffer for MODEM_TIMELIMIT 
                clear_time = 0
                # Start with the bad bytes that we have already read
                cleared_bytes = in_bytes
    
                while clear_time < MODEM_TIMELIMIT:
                    # Keep reading in more bytes 
                    cleared_waitbytes = self.modem.inWaiting()
                    cleared_bytes += self.modem.read(cleared_waitbytes)

                    # Sleep to let the modem return more data
                    time.sleep(MODEM_READ_PAUSE)
                    clear_time += MODEM_READ_PAUSE

                # Don't store any data. Just return with a trivial error
                # string
                res = "Crash in decoding: ERROR"
                return res

            # We have read all of the data
            if "OK" in res or "ERROR" in res:
                break

            # This is so we don't pull constantly
            time.sleep(MODEM_READ_PAUSE)
            # Increment the amount of time down in the read
            # This should be dominated by the pause time (so it is
            # a good approximation)
            modem_time += MODEM_READ_PAUSE

        # This is just some extra logging to signify that we cut off the
        # read early because we hit the modem time-limit
        if modem_time >= MODEM_TIMELIMIT:
            utils.log("Modem time went over")

        return res

    def format_at_command(self, command, command_arg1=None, command_arg2=None):
        if command not in AT_COMMANDS:
            raise Exception('The AT command you specified is unknown')

        at_command = AT_COMMANDS[command]

        if command_arg1 is not None:
            if command_arg2 is not None:
                format_cmd = at_command.format(command_arg1, command_arg2)
            else:
                format_cmd = at_command.format(command_arg1)

        else:
            format_cmd = at_command

        # In python3 it does not automatically convert the string into a byte format
        # the the serial interface uses. Thus, we need to convert it to bytes manually.
        return bytes(format_cmd + "\r\n", 'UTF-8')
