#ECL Embedded Couchbase plugin

This is the ECL plugin to access [Couchbase](http://www.couchbase.com/), an
open-source, distributed multi-model NoSQL document-oriented database software
package that is optimized for interactive applications. Freehand N1QL queries
can be embedded within your ECL query.

Client access is based on libcouchbase and libcouchbase-cxx.

##Installation and Dependencies

Both [libcouchbase](https://github.com/couchbase/libcouchbase) and
[libcouchbase-cxx](https://github.com/couchbaselabs/libcouchbase-cxx) are
included as git submodules within HPCC-Platform. They will be built and
integrated automatically when you build the HPCC-Platform project with the
couchbase plugin flag turned on.

##Plugin Configuration

The ECL Embedded couchbase plugin uses sensible default configuration values but these can
be modified via configuration parameters.

The accepted configuration parameters are as follows:

                Name                  Default Value
                server                "localhost"
                port                  8091
                password              ""
                bucket                "default"
                useSSL                false
                max_connections       0
                detailed_errcodes
                operation_timeout
                config_total_timeout
                http_poolsize

The `max_connections` parameter governs the maximum number of separate, active
connections for the exact combination of server, port, bucket parameters. A
value of zero indicates that there is no set maximum. This parameter may need
to be set if very large ECL workloads (such as handling many concurrent Roxie
requests) overwhelm the Couchbase server.

See the libcouchbase client settings documentation for appropriate values for
`detailed_errcodes`, `operation_timeout`, `config_total_timeout`, and `http_poolsize`.

Configuration parameters are declared as part of the ENDEMBED couchbase function definition.
For example:

     BOOLEAN  eventstatus(REAL m) := EMBED(couchbase : server(10.0.0.1), port(8091),  bucket('mybucket'), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
         ...
     ENDEMBED;

##N1QL Query Examples

Note that only a single N1QL statement embedded function is supported at this time.

                //Creates a primary index on 'mybucket' bucket
                createprimeindex() := EMBED(couchbase : server(server), port(port), bucket('mybucket'))
                    CREATE PRIMARY INDEX ON iot;
                ENDEMBED;

                //Simple select returning scalar string
                STRING scalarsimpleselect() := EMBED(couchbase : server(server), port(port), bucket('mybucket'))
                    SELECT timestamp from mybucket where timestamp = '2016-06-06 11:36:05.657314-04:00' LIMIT 1;
                ENDEMBED;

                //Simple parameterized select returning scalar numeric
                UNSIGNED parameterizedselectbool(BOOLEAN m) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                    SELECT COUNT(sequence) FROM mybucket WHERE isStaleData = $m; // boolean value 'm' passed in
                ENDEMBED;

                flatrec := RECORD
                    STRING latitude,
                    REAL4 longitude,
                    BOOLEAN isStaleData,
                    INTEGER sequence;
                END;

                // simple select returning flat record(s)
                DATASET(flatrec) flatdatasetpreparedselect(REAL m) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                    SELECT contextualData.gps.latitude,contextualData.gps.longitude, isStaleData, sequence FROM mybucket WHERE contextualData.gps.latitude = $m;
                ENDEMBED;

                XYRecord := RECORD
                    STRING x,
                    STRING y;
                END;

                //Function accepts row type
                BOOLEAN selectrow(row(XYRecord) values) := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                    SELECT eventStatus FORM mybucket WHERE accelx = $x AND accely = $y;
                ENDEMBED;

                FullRec := RECORD
                    REAL4 accelx;
                    REAL4 accely;
                    REAL4 accelz;
                    STRING4 eventId;
                    BOOLEAN eventStatus;
                    STRING guid;
                    BOOLEAN isStaleData;
                    INTEGER sequence;
                    INTEGER sourceoffset;
                    UNSIGNED sourcepartition;
                    STRING sourcetopic;
                    STRING timestamp;
                    DATASET(locationDatarec) locationData;
                END;

                //Full select
                DATASET(FullRec) fullselect() := EMBED(couchbase : server(server), port(port),  bucket('mybucket'))
                    SELECT mybucket.* FROM mybucket;
                ENDEMBED;
