import time
import sys
import datetime

def log(message, is_bytes = False, gps_data = None):
    '''This takes a string to be logged.
    
    This will include hte timestamp and the message and will log it.
    It also includes additional arguments to be able to print byte
    strings and gps_data. In the case where is_bytes is true, then
    gps_data will be ignored.

    Args:
        message (string): The message, as a string, to be logged.
        is_bytes (bool): It should be printed like a byte string 
        gps_data (dict): Additional data that needs to be formatted and printed.
    
    '''
    if is_bytes:
        print(message)
        sys.stdout.flush()
        return

    if gps_data is not None:

        message = message + ":\n"
        message += "  ept - estimated time stamp error: " + str(gps_data.get('ept', None)) + "\n"
        message += "  lat - latitude: " + str(gps_data.get('lat', None)) + "\n"
        message += "  lon - longitude: " + str(gps_data.get('lon', None)) + "\n"
        message += "  alt - altitude: " + str(gps_data.get('alt', None)) + "\n"
        message += "  epx - estimated error in longitude (m): " + str(gps_data.get('epx', None)) + "\n"
        message += "  epy - estimated error in latitude (m): " + str(gps_data.get('epy', None)) + "\n"
        message += "  epv - estimated vertical error (m): " + str(gps_data.get('epv', None)) + "\n"
        message += "  track - course over ground (deg from true N): " + str(gps_data.get('track', None)) + "\n"
        message += "  speed - speed over ground (m/s): " + str(gps_data.get('speed', None)) + "\n"
        message += "  climb - climb (+) or sink (-) (m/s): " + str(gps_data.get('climb', None)) + "\n"
        message += "  epd - direction error (deg): " + str(gps_data.get('epd', None)) + "\n"
        message += "  eps - speed error (m/s): " + str(gps_data.get('eps', None)) + "\n"
        message += "  epc - climb/sink error (m/s): " + str(gps_data.get('epc', None)) + "\n"
        message += "  mode (what kind of fix): " + str(gps_data.get('mode', None)) + "\n"
        message += "  time: " + str(gps_data.get('time', None))

    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(str(timestamp) + ": " + message)
    sys.stdout.flush()

def log_gps_time(time, mode):
    '''This will log the time that is given as a string (assumed GPS time)

    Args:
        time (string): The time to be logged
    '''
    if mode is None:
        print("GPS MODE: None")
    else:
        print("GPS MODE: " + mode)

    if time is None:
        print("GPS TIME: None")
    else:
        print("GPS TIME: " + time)

    sys.stdout.flush()

def get_sensor_name():
    '''Useful if you have multiple sensors and want to distinguish them
    for now this just returns a static name.

    Return:
        string: The name of the sensor
    '''

    return "seaglass-0"
