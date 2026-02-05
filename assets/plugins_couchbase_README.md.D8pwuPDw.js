import{_ as t,a,o,ag as n}from"./chunks/framework.Do1Zayaf.js";const d=JSON.parse('{"title":"","description":"","frontmatter":{},"headers":[],"relativePath":"plugins/couchbase/README.md","filePath":"plugins/couchbase/README.md","lastUpdated":1770295448000}'),c={name:"plugins/couchbase/README.md"};function r(s,e,i,u,l,p){return o(),a("div",null,e[0]||(e[0]=[n(`<p>#ECL Embedded Couchbase plugin</p><p>This is the ECL plugin to access <a href="http://www.couchbase.com/" target="_blank" rel="noreferrer">Couchbase</a>, an open-source, distributed multi-model NoSQL document-oriented database software package that is optimized for interactive applications. Freehand N1QL queries can be embedded within your ECL query.</p><p>Client access is based on libcouchbase and libcouchbase-cxx.</p><p>##Installation and Dependencies</p><p>Both <a href="https://github.com/couchbase/libcouchbase" target="_blank" rel="noreferrer">libcouchbase</a> and <a href="https://github.com/couchbaselabs/libcouchbase-cxx" target="_blank" rel="noreferrer">libcouchbase-cxx</a> are included as git submodules within HPCC-Platform. They will be built and integrated automatically when you build the HPCC-Platform project with the couchbase plugin flag turned on.</p><p>##Plugin Configuration</p><p>The ECL Embedded couchbase plugin uses sensible default configuration values but these can be modified via configuration parameters.</p><p>The accepted configuration parameters are as follows:</p><pre><code>            Name                  Default Value
            server                &quot;localhost&quot;
            port                  8091
            password              &quot;&quot;
            bucket                &quot;default&quot;
            useSSL                false
            max_connections       0
            detailed_errcodes
            operation_timeout
            config_total_timeout
            http_poolsize
</code></pre><p>The <code>max_connections</code> parameter governs the maximum number of separate, active connections for the exact combination of server, port, bucket parameters. A value of zero indicates that there is no set maximum. This parameter may need to be set if very large ECL workloads (such as handling many concurrent Roxie requests) overwhelm the Couchbase server.</p><p>See the libcouchbase client settings documentation for appropriate values for <code>detailed_errcodes</code>, <code>operation_timeout</code>, <code>config_total_timeout</code>, and <code>http_poolsize</code>.</p><p>Configuration parameters are declared as part of the ENDEMBED couchbase function definition. For example:</p><pre><code> BOOLEAN  eventstatus(REAL m) := EMBED(couchbase : server(10.0.0.1), port(8091),  bucket(&#39;mybucket&#39;), detailed_errcodes(1), operation_timeout(5.5), config_total_timeout(15))
     ...
 ENDEMBED;
</code></pre><p>##N1QL Query Examples</p><p>Note that only a single N1QL statement embedded function is supported at this time.</p><pre><code>            //Creates a primary index on &#39;mybucket&#39; bucket
            createprimeindex() := EMBED(couchbase : server(server), port(port), bucket(&#39;mybucket&#39;))
                CREATE PRIMARY INDEX ON iot;
            ENDEMBED;

            //Simple select returning scalar string
            STRING scalarsimpleselect() := EMBED(couchbase : server(server), port(port), bucket(&#39;mybucket&#39;))
                SELECT timestamp from mybucket where timestamp = &#39;2016-06-06 11:36:05.657314-04:00&#39; LIMIT 1;
            ENDEMBED;

            //Simple parameterized select returning scalar numeric
            UNSIGNED parameterizedselectbool(BOOLEAN m) := EMBED(couchbase : server(server), port(port),  bucket(&#39;mybucket&#39;))
                SELECT COUNT(sequence) FROM mybucket WHERE isStaleData = $m; // boolean value &#39;m&#39; passed in
            ENDEMBED;

            flatrec := RECORD
                STRING latitude,
                REAL4 longitude,
                BOOLEAN isStaleData,
                INTEGER sequence;
            END;

            // simple select returning flat record(s)
            DATASET(flatrec) flatdatasetpreparedselect(REAL m) := EMBED(couchbase : server(server), port(port),  bucket(&#39;mybucket&#39;))
                SELECT contextualData.gps.latitude,contextualData.gps.longitude, isStaleData, sequence FROM mybucket WHERE contextualData.gps.latitude = $m;
            ENDEMBED;

            XYRecord := RECORD
                STRING x,
                STRING y;
            END;

            //Function accepts row type
            BOOLEAN selectrow(row(XYRecord) values) := EMBED(couchbase : server(server), port(port),  bucket(&#39;mybucket&#39;))
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
            DATASET(FullRec) fullselect() := EMBED(couchbase : server(server), port(port),  bucket(&#39;mybucket&#39;))
                SELECT mybucket.* FROM mybucket;
            ENDEMBED;
</code></pre>`,16)]))}const m=t(c,[["render",r]]);export{d as __pageData,m as default};
