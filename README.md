# SeaGlass

This is the code to run a SeaGlass IMSI-catcher sensor. See https://seaglass.cs.washington.edu for more details on the SeaGlass project.

## Equipment Requirements

To run a SeaGlass sensor you need:

* Telit GT864-quad modem
* USB GPS device that works over a serial interface (and GPSD)
* Linux machine (e.g., Ubuntu laptop, raspberry pi, etc.)

## Sensor Installation

### GPS

Currently we only support linux.

Begin by configuring the GPS. First install `gspd` with:

```
$ sudo apt-get install gpsd
```

To prevent interference with the telit modem, you will need to first disable auto connect mode in gpsd. Instead, you will need to hard code the path to the serial device. Plug in the GPS device and locate the path to the device (probably /dev/ttyUSB0). Once you locate the path to the GPS device run:

```
$ sudo dpkg-reconfigure gpsd
```
Disable automatically handling attached USB GPS receiver and then hard code the path to the GPS receiver using the path to the device you just located.

If `dpkg-reconfigure` is not working you may have to manually edit the configuration file for `gpsd`

Finally, to make sure the process has access to connect to the serial device, add your user to the dialout group.

```
$ usermod -a -G dialout <your-username>
```

For the group change to take effect, you will need to re-login.

### MongoDB

The data from the cellular scan and GPS is automatically written to a MongoDB database (dbname = SensorDB and collection=Scan).

This requires that MongoDB is installed and accepting connections on localhost. To do this just run:

```
$ sudo apt-get install mongodb-server
```

### Python configuration

You also need to install python3, and the following python packages (we suggest using pip):

* `pySerial`
* `pymongo`

### Postgres Support (Optional)

We also include code to transform the data from Mongo to Postgresql, where it is easier to analyze. To do this you must first install postgres. Then edit the fields specified in the `postgres_config` file.

## Run

To run the cellular survey make sure the modem and GPS are plugged in. Then locate the path to the modem serial device (probably /dev/ttyUSB1 if it was plugged in after the GPS).

```
./survey.py <path-to-modem-serial-device>'
```

This will run the survey and write data to the local MongoDB.

If you want to convert the mongo database to postgres run

```
./mongo2postgres.py
```
