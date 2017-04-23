import common.lib.gps_python3 as gps
import threading
import time
import copy

# This is the longest amount of time that we will accept a GPS value.
# If they are any older then we will ignore them.
# This time is in seconds.
GPS_FRESH = 2

# This class is required to pull from gpsd
class GpsScanner(threading.Thread):

    def __init__(self):
        # Initialize the thread
        threading.Thread.__init__(self)

        # This is the object that will be collecting the gps data
        self.session = gps.GPS(verbose=False)
        self.session.stream(gps.WATCH_ENABLE)

        # We need this for synchronization
        # Just a standard re-entrant lock
        self.lock = threading.RLock()

        # Initialize the current value to None
        self.set_cur_value(None)

        # Begin the collection thread
        self.start()

    def set_cur_value(self, val):
        self.lock.acquire()
        self.value = val
        self.lock.release()

    def get_cur_value(self):
        self.lock.acquire()
        if self.value is not None:
            val = copy.deepcopy(self.value)
        else:
            val = None
        self.lock.release()

        return val

    # Keep pulling from gpsd so the socket doesn't fill
    def run(self):

        n_since_tpv = 0

        while True:
            try:
                # Update the current value
                next_val = self.session.next()

                # We are only interested in TPV measurements
                # There is some chance that we will get back some weird data
                # So the first two checks make this more roboust
                if next_val is None or           \
                      'class' not in next_val or    \
                      next_val['class'] != 'TPV':

                    bad_read = True
                    n_since_tpv = n_since_tpv + 1

                else:
                    bad_read = False
                    n_since_tpv = 0

                # The read is bad and now the data is old so make it None
                # If the data is bad but not enough loops for tpv then don't
                # update anything
                if bad_read and n_since_tpv >= 4:
                   self.set_cur_value(None)
                elif not bad_read:
                    # The value is good so update it
                    self.set_cur_value(next_val)

                # This means that we have 4 chances to get a TPV value before we declare that
                # it is too old
                time.sleep(float(GPS_FRESH) / 4)

            # Occasionally if you pull too quickly (i.e., before the GPS has any info)
            # then the gps library will throw an exception (which we want to ignore)
            except StopIteration:
                pass

    # This should return the most recent datapoint, with time formatted nicely.
    def scan(self):
        # raw gps data
        gps_data = self.get_cur_value()

        # There is nothing to parse so just return
        if gps_data == None:
            # We want to create an empty dict so that the insert will work correctly
            gps_data = {}
    
        if 'time' in gps_data:
            gps_data['time'] = gps_data['time'].replace('T', ' ')[:-1]
    
        return gps_data


