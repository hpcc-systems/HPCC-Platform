/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=embedded
//class=3rdparty

IMPORT couchbase;
IMPORT STD;

/***********************************************************************************************
   This set of example queries are based on a device oriented document structure described below
   These samples queries rely on a bucket named 'iot'. In order to run these queries, please
      create the iot bucket on the target couchbase server
************************************************************************************************/

server := '127.0.0.1';
thebucket := 'iot';

string adocid := '"mydocid' ;
string adoc1  := '{"deviceID": "9cf9r-3c0f-446b-ad6a-4136deb88519" ,"deviceType": "handheld","message":{"locationData":{"x": 52.227,"y": 77.699,"z": 99.999, "zoneId": "W"}, "rawdata":{"ambientTemp": 68.21, "barometer": 14.81, "batteryLevelPercentage": 95.32, "bodyTemp": 30.08, "coLevel": 1.46, "forceSensitiveResistance": 136, "heartRate": 94}, "deviceTimestamp": "2017-06-05 15:04:35.924657-0400"}, "receivedTimestamp": "' + STD.System.Debug.msTick ( ) + '"}';
string adoc2 := '{"deviceID":  "1135c-3c0f-776b-ag6a-7136deb83464" ,"deviceType": "wearable","message":{"locationData":{"x": 2.247,"y": 33.629,"z": 1.323,   "zoneId": "A"}, "rawdata":{"ambientTemp": 8.65,  "barometer": 34.56, "batteryLevelPercentage": 32.09, "bodyTemp": 29.68, "coLevel": 1.34, "forceSensitiveResistance": 6,   "heartRate": 56}, "deviceTimestamp": "2017-06-05 15:05:27.325365-0400"}, "receivedTimestamp": "' + STD.System.Debug.msTick ( ) + '"}';

/*
Document structure:
{
    "deviceID": "9cf9r-3c0f-446b-ad6a-4136deb88519",
    "deviceType": "handheld",
    "message":
    {
        "locationData": {
            "x": 52.227,
            "y": 77.699,
            "z": 99.999,
            "zoneId": "W"
        },
        "rawdata": {
            "ambientTemp": 38.21,
            "barometer": 14.81,
            "batteryLevelPercentage": 95.32,
            "bodyTemp": 30.08,
            "coLevel": 1.46,
            "forceSensitiveResistance": 166,
            "heartRate": 94
        },
        "deviceTimestamp": "2017-06-05 15:04:35.924657-0400"
    },
    "receivedTimestamp": "1496275475926453"
}
*/

deviceidrec := RECORD
  string deviceID;
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

locationDatarec := RECORD
  real x {xpath('x')};
  real y {xpath('y')};
  real z {xpath('z')};
  string zoneId {xpath('zoneId')};
END;

messagerec := RECORD
  string deviceTimestamp;
  locationDatarec locationData;
  rawdatarec rawdata;
END;

iotrec := RECORD
  string deviceID;
  string deviceType;
  string receivedTimestamp;
  messagerec message;
END;

BOOLEAN insertiotdoc(string docid, string doc) := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1))
  INSERT INTO iot (KEY, VALUE) VALUES ($docid, $doc) RETURNING TRUE AS c;
ENDEMBED;

BOOLEAN upsertiotdoc(string docid, string doc) := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1))
  INSERT INTO iot (KEY, VALUE) VALUES ($docid, $doc) RETURNING TRUE AS c;
ENDEMBED;

STRING getlatesttimestamp() := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1))
  SELECT max(iot.receivedTimestamp) FROM iot;
ENDEMBED;

dataset(iotrec) getlatestdoc(string devid) := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1))
  SELECT iot.* FROM iot where iot.deviceID = $devid;
ENDEMBED;

integer devicecount() := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1))
  SELECT count(DISTINCT iot.deviceID) FROM iot ;
ENDEMBED;

dataset(iotrec) devicebylocation(row(locationDatarec) values) := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select iot.* from iot where iot.message.locationData.x = $x and iot.message.locationData.y = $y limit 1;
ENDEMBED;

dataset(deviceidrec) deviceidsbyzone(string zone) := EMBED(couchbase : server(server), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select DISTINCT iot.deviceID from iot where iot.message.locationData.zoneId = $zone;
ENDEMBED;

sequential
(
  OUTPUT(insertiotdoc(adocid + random() + '"', adoc1));
  Std.System.Debug.Sleep(200);
  OUTPUT(insertiotdoc(adocid + random() + '"', adoc2));
  OUTPUT(upsertiotdoc(adocid + random() + '"', adoc1));
  Std.System.Debug.Sleep(200);
  OUTPUT(getlatesttimestamp());
  OUTPUT(getlatestdoc('"11135e49c-3c0f-446b-ad6a-4136deb88519"'));
  OUTPUT(devicecount());
  OUTPUT(devicebylocation(ROW({2.247,33.629,1.323,'"A"'},locationDatarec)));
  OUTPUT(deviceidsbyzone('"A"'));
  OUTPUT('Done');
);