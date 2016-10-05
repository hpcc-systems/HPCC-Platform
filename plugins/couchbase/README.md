#ECL Embedded Couchbase plugin

This is the ECL plugin to access Couchbase](http://www.couchbase.com/), an
open-source, distributed multi-model NoSQL document-oriented database software
package that is optimized for interactive applications. Freehand N1QL queries
can be embedded within your ECL query.

Client access is based on libcouchbase (c interface provided by couchbase install) and
libcouchbase-cxx (from https://github.com/couchbaselabs/libcouchbase-cxx)

##Installation and Dependencies

[libcouchbase] is installed via standard package managers


(https://github.com/couchbaselabs/libcouchbase-cxx) is included as a git
submodule in HPCC-Platform.  It will be built and integrated automatically when
you build the HPCC-Platform project with the couchbase plugin flag turned on.

The recommended method for obtaining libcouchbase is via
On Ubuntu: sudo apt-get install libcouchbase-dev libcouchbase2-bin build-essential

##Plugin Configuration
 
The ECL Embedded couchbase plugin uses sensible default configuration values but these can
be modified via configuration parameters.
 
The accepted configuration parameters are as follows:
                Name                  Default Value
                server                "localhost"
                port                  8091
                user                  ""
                password              ""
                bucket                "default"
                useSSL                false
 
                detailed_errcodes
                operation_timeout
                config_total_timeout
                http_poolsize
 
Configuration parameters are declared as part of the ENDEMBED couchbase funtion definition.
For example:
     boolean  eventstatus(REAL m) := EMBED(couchbase : server(10.0.0.1), port(8091),  bucket('mybucket'), user('myname'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
         ...
     ENDEMBED;

##N1QL Query Examples
                //Creates a primary index on 'mybucket' bucket
                createprimeindex() := EMBED(couchbase : server(server), port(port), bucket('mybucket'))
                                create primary index on iot;
                ENDEMBED;
 
                //Simple select returning scalar string
                string scalarsimpleselect() := EMBED(couchbase : server(server), port(port), bucket('mybucket'))
                                SELECT timestamp from mybucket where timestamp = '2016-06-06 11:36:05.657314-04:00' limit 1;
                ENDEMBED;
 
                //Simple parameterized select returning scalar numeric
                unsigned parameterizedselectbool(boolean m) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                                SELECT count(sequence) from mybucket where isStaleData = $m; // boolean value 'm' passed in
                ENDEMBED;
 
                flatrec := RECORD
        string latitude,
                                real4 longitude,
                                boolean isStaleData,
                                INTEGER sequence;
                END;
               
                // simple select returning flat record(s)
                dataset(flatrec) flatdatasetpreparedselect(REAL m) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                                SELECT contextualData.gps.latitude,contextualData.gps.longitude, isStaleData, sequence from mybucket where contextualData.gps.latitude = $m;
                ENDEMBED;
 
                XYRecord := RECORD
                    STRING x,
                    STRING y;
                END;
 
                //Function accepts row type
                boolean selectrow(row(XYRecord) values) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                                SELECT eventStatus from mybucket where accelx = $x and accely = $y;
                ENDEMBED;
 
                FullRec := RECORD
                                real4 accelx;
                                real4 accely;
                                real4 accelz;
                                string4 eventId;
                                boolean eventStatus;
                                string guid;
                                boolean isStaleData;
                                integer sequence;
                                integer sourceoffset;
                                unsigned sourcepartition;
                                string sourcetopic;
                                string timestamp;
                                dataset(locationDatarec) locationData;
                END;
 
                //Full select
                dataset(FullRec) fullselect() := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                                SELECT mybucket.* from mybucket;
                ENDEMBED;
