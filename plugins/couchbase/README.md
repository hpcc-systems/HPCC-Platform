#ECL Embedded Couchbase plugin

This is the ECL plugin to access couchbase systems

Client access is based on libcouchbase (c interface provided by couchbase install) and
libcouchbase-cxx (from https://github.com/couchbaselabs/libcouchbase-cxx)

##Installation and Dependencies

[libcouchbase] is installed via standard package managers


(https://github.com/couchbaselabs/libcouchbase-cxx) is included as a git
submodule in HPCC-Platform.  It will be built and integrated automatically when
you build the HPCC-Platform project with the couchbase plugin flag turned on.

The recommended method for obtaining libcouchbase is via
On Ubuntu: sudo apt-get install libcouchbase-dev libcouchbase2-bin build-essential

##ECL Plugin Use
This is a WIP plugin, currently only exposing a general 'executen1ql' interface

Examples:
IMPORT * FROM plugins.couchbase;

server := '127.0.0.1';

ECLGPS := RECORD
    string latitude,
    string longitude;
END;

flatiotrec := RECORD
    string latitude,
    real4 longitude,
    boolean isStaleData,
    INTEGER sequence;
END;

rawdatarec := RECORD
    real4 ambientTemp;
    real4 barometer;
    real4 batteryLevelPercentage;
    real4 bodyTemp;
    real4 coLevel;
    real4 forceSensitiveResistance;
    INTEGER heartRate;
END;

fulliotrec := RECORD
   real4 accelx;
	 real4 accely;
	 real4 accelz;
	 //ECLGPS gps;
	 //dataset(contextualDatarec) contextualData;
	 string4 eventId;
	 boolean eventStatus;
	 string guid;
	 boolean isStaleData;
	 integer sequence;
	 integer sourceoffset;
	 unsigned sourcepartition;
	 string sourcetopic;
	 string timestamp;
	 //dataset(locationDatarec) locationData;
	 dataset(rawdatarec) rawData;
END;


createprimeindex() := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  create primary index on iot;
ENDEMBED;

string scalarsimpleselect() := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT timestamp from iot where timestamp = '2016-01-07 11:36:05.657314-04:00' ;
ENDEMBED;

string invalidquery() := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT x from iot;
ENDEMBED;

string simplefilteredselect() := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps from iot where contextualData.gps.latitude = 44.968046;
ENDEMBED;

unsigned parameterizedselectbool(boolean m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT count(accelx) from iots where accelx = $m ;
ENDEMBED;

string parameterizedselect(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps.longitude from iot where contextualData.gps.latitude = $m;
ENDEMBED;

 INTEGER selectnegativeint (REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
   select sourceoffset  from  iot where sourceoffset < $m;
ENDEMBED;

INTEGER preparedselectint(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
     SELECT sequence from iot where contextualData.gps.latitude = $m;
ENDEMBED;

REAL preparedselectreal(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps.longitude from iot where contextualData.gps.latitude = $m;
ENDEMBED;

boolean preparedselectboolean(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT isStaleData from iot where contextualData.gps.latitude = $m;
ENDEMBED;

dataset(flatiotrec) flatdatasetpreparedselect(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps.latitude,contextualData.gps.longitude, isStaleData, sequence  from iot where contextualData.gps.latitude = $m;
ENDEMBED;

dataset(iotrec) datasetpreparedselect(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps  from iot where contextualData.gps.latitude = $m;
ENDEMBED;

//embeded couchbase call results in odd ECL error indicating a missing plugin function, but the function exists...
//Error:  no matching function for call to ‘IEmbedFunctionContext::getRowResult()’ (35, 32 - W20160701-104659_1.cpp)
//Error:  Compile/Link failed for W20160701-104659 (see '//10.0.2.15/var/lib/HPCCSystems/myeclccserver/eclcc.log' for details) (0, 0 - W20160701-104659)
row rowpreparedselect(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT contextualData.gps from iot where contextualData.gps.latitude = $m;
ENDEMBED;

string stringselecteventid(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT eventId from iot where contextualData.gps.latitude = $m;
ENDEMBED;

string  dataselecteventid(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT timestamp from iot where contextualData.gps.latitude = $m;
ENDEMBED;

boolean  boolselecteventstatus(REAL m) := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT eventStatus from iot where contextualData.gps.latitude = $m;
ENDEMBED;

dataset(fulliotrec) fullselect() := EMBED(couchbase : server(server), bucket('iot'), user('rpastrana'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT iot.* from iot;
ENDEMBED;

sequential (
  createprimeindex(),
  OUTPUT(parameterizedselectbool(1)),
  OUTPUT(scalarsimpleselect()),
  OUTPUT(parameterizedselect(44.968046)),
  OUTPUT(selectnegativeint(0)),
  output(preparedselect(44.968046)),
  OUTPUT(stringselecteventid(44.968046)),
  OUTPUT(preparedselectint(44.968046)),
  OUTPUT(rowpreparedselect(44.968046)),
  OUTPUT(flatdatasetpreparedselect(44.968046)),
  OUTPUT(datasetpreparedselect(44.968046)),
  Count(datasetpreparedselect(44.968046)),
  OUTPUT(parameterizedselect(44.968046)),
  OUTPUT(fullselect())
  OUTPUT('Done');
);
