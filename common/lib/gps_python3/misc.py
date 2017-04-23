# misc.py - miscellaneous geodesy and time functions
#
# This file is Copyright (c) 2010 by the GPSD project
# BSD terms apply: see the file COPYING in the distribution root for details.

import time
import calendar
import math

# some multipliers for interpreting GPS output
METERS_TO_FEET = 3.2808399  	 # Meters to U.S./British feet
METERS_TO_MILES = 0.00062137119	 # Meters to miles
METERS_TO_FATHOMS = 0.54680665	 # Meters to fathoms
KNOTS_TO_MPH = 1.1507794	     # Knots to miles per hour
KNOTS_TO_KPH = 1.852		     # Knots to kilometers per hour
KNOTS_TO_MPS = 0.51444444	     # Knots to meters per second
MPS_TO_KPH = 3.6		         # Meters per second to klicks/hr
MPS_TO_MPH = 2.2369363	         # Meters/second to miles per hour
MPS_TO_KNOTS = 1.9438445	     # Meters per second to knots

# EarthDistance code swiped from Kismet and corrected


def degree_to_radian(x):
    """ Degrees to radians. """
    return x * (math.pi/180)


def radian_to_degree(x):
    """ Radians to degrees. """
    return x * (180/math.pi)


def calc_rad(lat):
    """ Radius of curvature in meters at specified latitude. """
    a = 6378.137
    e2 = 0.081082 * 0.081082
    # the radius of curvature of an ellipsoidal Earth in the plane of a
    # meridian of latitude is given by
    #
    # R' = a * (1 - e^2) / (1 - e^2 * (sin(lat))^2)^(3/2)
    #
    # where a is the equatorial radius,
    # b is the polar radius, and
    # e is the eccentricity of the ellipsoid = sqrt(1 - b^2/a^2)
    #
    # a = 6378 km (3963 mi) Equatorial radius (surface to center distance)
    # b = 6356.752 km (3950 mi) Polar radius (surface to center distance)
    # e = 0.081082 Eccentricity
    sc = math.sin(degree_to_radian(lat))
    x = a * (1.0 - e2)
    z = 1.0 - e2 * sc * sc
    y = pow(z, 1.5)
    r = x / y

    r *= 1000.0      # Convert to meters
    return r


def earth_distance(lat1, lon1, lat2, lon2):
    """ Distance in meters between two points specified in degrees. """
    x1 = calc_rad(lat1) * math.cos(degree_to_radian(lon1)) * math.sin(degree_to_radian(90-lat1))
    x2 = calc_rad(lat2) * math.cos(degree_to_radian(lon2)) * math.sin(degree_to_radian(90-lat2))
    y1 = calc_rad(lat1) * math.sin(degree_to_radian(lon1)) * math.sin(degree_to_radian(90-lat1))
    y2 = calc_rad(lat2) * math.sin(degree_to_radian(lon2)) * math.sin(degree_to_radian(90-lat2))
    z1 = calc_rad(lat1) * math.cos(degree_to_radian(90-lat1))
    z2 = calc_rad(lat2) * math.cos(degree_to_radian(90-lat2))
    a = (x1*x2 + y1*y2 + z1*z2)/pow(calc_rad((lat1+lat2)/2), 2)
    # a should be in [1, -1] but can sometimes fall outside it by
    # a very small amount due to rounding errors in the preceding
    # calculations (this is prone to happen when the argument points
    # are very close together).  Thus we constrain it here.
    if abs(a) > 1:
        a = 1
    elif a < -1:
        a = -1
    return calc_rad((lat1+lat2) / 2) * math.acos(a)


def meter_offset(lat1, lon1, lat2, lon2):
    """ Return offset in meters of second arg from first. """
    dx = earth_distance(lat1, lon1, lat1, lon2)
    dy = earth_distance(lat1, lon1, lat2, lon1)
    if lat1 < lat2: dy *= -1
    if lon1 < lon2: dx *= -1
    return dx, dy


def iso_time(s):
    """ Convert timestamps in ISO8661 format to and from Unix time. """
    if isinstance(s, int):      # changed from type(s) == type(1)
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(s))
    elif isinstance(s, float):  # changed from type(s) == type(1.0)
        date = int(s)
        msec = s - date
        date = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(s))
        return date + "." + repr(msec)[3:]
    elif isinstance(s, str):  # changed from type(s) == type("")
        if s[-1] == "Z":
            s = s[:-1]
        if "." in s:
            (date, msec) = s.split(".")
        else:
            date = s
            msec = "0"
        # Note: no leap-second correction! 
        return calendar.timegm(time.strptime(date, "%Y-%m-%dT%H:%M:%S")) + float("0." + msec)
    else:
        raise TypeError

# End

