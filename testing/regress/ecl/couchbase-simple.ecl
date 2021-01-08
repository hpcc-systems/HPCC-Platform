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
//class=3rdpartyservice

IMPORT couchbase;

/******************************************************************************************
   This set of example queries are based on the couchbase sample bucket 'travel-sample'
   The sample data is easily set-up, please refer to the couchbase documentation for set-up
*******************************************************************************************/

/*
 * change this to point to your couchbase
 * or use '-X CouchbaseServerIp=<your_couchbase_ip_address>' parameter in your CLI
 * to pass server address
 */
server := '127.0.0.1' : STORED('CouchbaseServerIp');
thebucket := 'travel-sample';
user := 'traveluser' : STORED('CouchbaseBucketUser');
password := 'travelpass' : STORED('CouchbaseBucketPass');

namerec := RECORD
  string name;
END;

typerec := RECORD
  string type;
END;

travelrec := RECORD
  string callsign;
  string country;
  string iata;
  string icao;
  integer id;
  string name;
  string type;
END;

georec := RECORD
  integer alt;
  real lat;
  real lon;
END;

airportrec := RECORD
  string airportname;
  string city;
  string country;
  string faa;
  string  tz;
  string type;
  integer id;
  string icao;
  georec geo;
END;

/*
 --Sample Airport record--
 "airportname": "Calais Dunkerque",
    "city": "Calais",
    "country": "France",
    "faa": "CQF",
    "geo": {
      "alt": 12,
      "lat": 50.962097,
      "lon": 1.954764
    },
    "icao": "LFAC",
    "id": 1254,
    "type": "airport",
    "tz": "Europe/Paris"
*/
/*
  --sample airline record--
  "callsign": null,
  "country": "United States",
  "iata": "WQ",
  "icao": "PQW",
  "id": 13633,
  "name": "PanAm World Airways",
  "type": "airline"
*/
schedrec := RECORD
  unsigned day;
  string flight;
  string utc;
END;

routerec := RECORD
  string airline;
  string airlineid;
  string3 destinationairport;
  real distance;
  string equipment;
  integer id;
  dataset(schedrec) schedule {xpath('schedule')};
  string3 sourceairport;
  unsigned stops;
  string type;
END;
/*
  "travel-sample": {
  "airline": "AH",
  "airlineid": "airline_794",
  "destinationairport": "CDG",
  "distance": 1420.6731433915318,
  "equipment": "738",
  "id": 10041,
  "schedule": [
      {
       "day": 0,
       "flight": "AH547",
       "utc": "07:58:00"
      },
      {
       "day": 0,
       "flight": "AH428",
       "utc": "12:08:00"
      },
      {
       "day": 1,
       "flight": "AH444",
       "utc": "14:16:00"
      },
      {
       "day": 3,
       "flight": "AH741",
       "utc": "04:56:00"
      },
      {
       "day": 4,
       "flight": "AH027",
       "utc": "03:16:00"
      },
      {
       "day": 4,
       "flight": "AH113",
       "utc": "08:11:00"
      },
      {
       "day": 6,
       "flight": "AH260",
       "utc": "18:02:00"
      },
      {
       "day": 6,
       "flight": "AH873",
       "utc": "03:58:00"
      }
     ],
     "sourceairport": "AAE",
     "stops": 0,
     "type": "route"
    }
    */

/* Due to inconsistencies found in couchbase travel-sample, some queries explicitly omit records of type landmark and hotel */

integer countAllRecords() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT count(*) from `travel-sample` as mybucketalias where mybucketalias.type != 'landmark' and mybucketalias.type != 'hotel';
ENDEMBED;

integer countTypes() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT COUNT(DISTINCT mybucketalias.type) from `travel-sample` as mybucketalias where mybucketalias.type != 'landmark' and mybucketalias.type != 'hotel';
ENDEMBED;

dataset(typerec) allTypes() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT DISTINCT mybucketalias.type from `travel-sample` as mybucketalias where mybucketalias.type IS NOT NULL and mybucketalias.type != 'landmark' and mybucketalias.type != 'hotel';
ENDEMBED;

dataset(travelrec) fulltravelrecords() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where callsign = 'MILE-AIR' limit 1;
ENDEMBED;

dataset(namerec) airlinenames() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.name from `travel-sample` as mybucketalias where mybucketalias.type = 'airline' limit 1;
ENDEMBED;

dataset(airportrec) airportrecords() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' limit 1;
ENDEMBED;

dataset(airportrec) usairportrecords() := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' and country = 'United States' limit 1;
ENDEMBED;

dataset(airportrec) airportrecordsbycountry(string m) := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' and country = $m limit 1;
ENDEMBED;

dataset(airportrec) airportrecordsbyid(integer id) := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' and id = $id limit 1;
ENDEMBED;

dataset(airportrec) airportrecordsbyaltitude(integer alt) := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' and geo.alt >= $alt limit 1;
ENDEMBED;

dataset(airportrec) airportrecordsbycoordinates(row(georec) values) := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  select mybucketalias.* from `travel-sample` as mybucketalias where mybucketalias.type = 'airport' and geo.lat = $lat and geo.lon = $lon limit 10;
ENDEMBED;

dataset(routerec) fullroute()  := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT mybucketalias.* FROM `travel-sample` as mybucketalias WHERE type = 'route' and sourceairport IS NOT NULL LIMIT 1
ENDEMBED;

dataset(routerec) routeschedule(string sair, string dair)  := EMBED(couchbase : server(server), user(user), password(password), bucket(thebucket), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
  SELECT mybucketalias.schedule  FROM `travel-sample` as mybucketalias WHERE type = 'route' and sourceairport = $sair and destinationairport = $dair  LIMIT 1
ENDEMBED;

sequential
(
  OUTPUT(countAllRecords(), NAMED('CountOfAllRecs'));
  OUTPUT(countTypes(), NAMED('CountOfAllRecTypes'));
  OUTPUT(allTypes(), NAMED('AllRecordTypes'));
  OUTPUT(airlinenames(), NAMED('AirlineNames'));
  OUTPUT(usairportrecords(), NAMED('USAirports'));
  OUTPUT(airportrecordsbycountry('"France"'), NAMED('AirportsByCountry'));
  OUTPUT(fulltravelrecords(), NAMED('FullTravelRec'));
  OUTPUT(airportrecordsbyid(3411), NAMED('AirportByID'));
  OUTPUT(airportrecordsbyaltitude(4000), NAMED('AirportsAtAltitude'));
  OUTPUT(airportrecordsbycoordinates(ROW({0,31.3426028, -109.5064544},georec)), NAMED('AirportAtCoordinate'));
  //OUTPUT(fullroute(), NAMED('FullRouteRec'));
  //OUTPUT(routeschedule('"AAE"', '"CDG"'),  NAMED('RouteAAEtoCDG'));
  OUTPUT('Done', NAMED('Status'));
);

