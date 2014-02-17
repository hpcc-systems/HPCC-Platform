Overview of Regression Suite usage
==================================
To use Regression Suite change directory to HPCC-Platform/testing/regress subdirectory.

Regression Suite requires Python environment version >=2.6.6 and < 3.x

Parameters of Regression Suite:
-------------------------------

Command:
 
    ./regress <-h|--help>

Result:

|
|       usage: regress [-h] [--version] [--config [CONFIG]]
|                       [--loglevel [{info,debug}]] [--suiteDir [SUITEDIR]]
|                       [--timeout [TIMEOUT]] [--keyDir [KEYDIR]]
|                       [--ignoreResult]
|                       {list,run,query} ...
| 
|       HPCC Platform Regression suite
| 
|       positional arguments:
|          {list,run,query}      sub-command help
|            list                list help
|            run                 run help
|            query               query help
|
|       optional arguments:
|            -h, --help            show this help message and exit
|            --version, -v         show program's version number and exit
|            --config [CONFIG]     config file to use. Default: regress.json.
|            --loglevel [{info,debug}]
|                                  set the log level. Use debug for more detailed logfile.
|            --suiteDir [SUITEDIR], -s [SUITEDIR]
|                                  suiteDir to use. Default value is the current directory and it can handle relative path.
|            --timeout [TIMEOUT], -t [TIMEOUT]
|                                  timeout for query execution in sec. Use -1 to disable timeout. Default value defined in regress.json config file (see: 7.)
|            --keyDir [KEYDIR], -k [KEYDIR]
|                                  key file directory to compare test output. Default value defined in regress.json config file.
|            --ignoreResult, -i    completely ignore the result.

Parameters of Regression Suite list sub-command:
------------------------------------------------

Command:

    ./regress list <-h|--help>

Result:

|
|       usage: regress list [-h]
|
|       positional arguments:
|         clusters    Print clusters from config (regress.json by default).
|
|       optional arguments:
|         -h, --help  show this help message and exit
|

Parameters of Regression Suite run sub-command:
-----------------------------------------------

Command:

    ./regress run <-h|--help>

Result:

|
|       usage: regress run [-h] [--pq threadNumber] [cluster]
|
|       positional arguments:
|         cluster            Run the cluster suite. Default value is setup.
|
|       optional arguments:
|         -h, --help         show this help message and exit
|         --pq threadNumber  Parallel query execution with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumer = numberOfLocalCore * 2)
|


Parameters of Regression Suite query sub-command:
-------------------------------------------------

Command:

    ./regress query <-h|--help>

Result:

|
|       usage: regress query [-h] [--publish] [ECL query] [target cluster | all]
|
|       positional arguments:
|         ECL query             Name of a single ECL query. It can contain wildcards. (mandatory).
|         target cluster | all  Cluster for single query run. If cluster = 'all' then run single query on all clusters. Default value is thor.
|
|       optional arguments:
|         -h, --help            show this help message and exit
|         --publish             Publish compiled query instead of run.



Steps to run Regression Suite
=============================

1. Change directory to HPCC-Platform/testing/regress subdirectory.
------------------------------------------------------------------

2. To list all available clusters:
----------------------------------
Command:

    ./regress list

The result looks like this:

        Available Clusters: 
            - setup
            - hthor
            - thor
            - roxie



3. To run the Regression Suite setup:
-------------------------------------

Command:

        ./regress run

or

        ./regress run setup

The result:

|
|        [Action] Suite: setup
|        [Action] Queries: 3
|        [Action] 1. Test: setup_fetch.ecl
|        [Pass] Pass W20130617-095047
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-095047`
|        [Action] 2. Test: setupxml.ecl
|        [Pass] Pass W20130617-095049
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-095049`
|        [Action] 3. Test: setup.ecl
|        [Pass] Pass W20130617-095051
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-095051`
|        [Action]
|
|            Results
|            `-------------------------------------------------`
|            Passing: 3
|            Failure: 0
|            `-------------------------------------------------`
|            Log: /var/log/HPCCSystems/regression/setup.13-06-17-09-50.log
|            `-------------------------------------------------`




4. To run Regression Suite on a selected cluster (e.g. Thor):
-------------------------------------------------------------
Command:

        ./regress run cluster [-h] [--pq threadNumber]

Positional arguments:
  cluster            Run the cluster suite (default: setup).

Optional arguments:
  -h, --help         show help message and exit
  --pq threadNumber  Parallel query execution with threadNumber threads.
                    ('-1' can be use to calculate usable thread count on a single node system)

The result is a list of test cases and their result. 

The first and last couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 257
|        [Action]
|        [Action]   1. Test: agglist.ecl
|        [Pass]   1. Pass W20131119-173524 (2 sec)
|        [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-173524
|        [Action]   2. Test: aggregate.ecl
|        [Pass]   2. Pass W20131119-173527 (1 sec)
|        [Pass]   2. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-173527
|        [Action]   3. Test: aggsq1.ecl
|
|        .
|        .
|        .
|        [Action] 256. Test: xmlout2.ecl
|        [Pass] Pass W20131119-182536 (1 sec)
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-182536
|        [Action] 257. Test: xmlparse.ecl
|        [Pass] Pass W20131119-182537 (1 sec)
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-182537
|
|         Results
|         `-------------------------------------------------`
|         Passing: 257
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /home/ati/HPCCSystems-regression/log/thor.13-11-19-17-52-27.log
|         `-------------------------------------------------`
|         Elapsed time: 1992 sec  (00:33:12)
|         `-------------------------------------------------`
|

If --pq option used (in this case with 16 threads) then then the content of the console log will be different like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 257
|        [Action]
|        [Action]   1. Test: agglist.ecl
|        [Action]   2. Test: aggregate.ecl
|        [Action]   3. Test: aggsq1.ecl
|        [Action]   4. Test: aggsq1seq.ecl
|        [Action]   5. Test: aggsq2.ecl
|        [Action]   6. Test: aggsq2seq.ecl
|        [Action]   7. Test: aggsq4.ecl
|        [Action]   8. Test: aggsq4seq.ecl
|        [Action]   9. Test: alljoin.ecl
|        [Action]  10. Test: apply3.ecl
|        [Action]  11. Test: atmost2.ecl
|        [Action]  12. Test: bcd1.ecl
|        [Action]  13. Test: bcd2.ecl
|        [Action]  14. Test: bcd4.ecl
|        [Action]  15. Test: betweenjoin.ecl
|        [Action]  16. Test: bigrecs.ecl
|        [Pass]   2. Pass W20131119-150514 (4 sec)
|        [Pass]   2. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-150514
|        [Pass]   1. Pass W20131119-150513 (4 sec)
|        [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-150513
|        [Action]  17. Test: bloom2.ecl
|        [Action]  18. Test: bug8688.ecl
|        [Pass]   3. Pass W20131119-150514-5 (5 sec)
|        [Pass]   3. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-150514-5
|        [Action]  19. Test: builtin.ecl
|        [Pass]  12. Pass W20131119-150517 (5 sec)
|        [Pass]  12. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-150517
|        [Action]  20. Test: casts.ecl
|        [Pass]  14. Pass W20131119-150517-2 (6 sec)
|        [Pass]  14. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-150517-2
|        [Action]  21. Test: catchexpr.ecl
|        .
|        .
|        .
|        [Action] 257. Test: xmlparse.ecl
|        [Pass] 240. Pass W20131119-160614 (9 sec)
|        [Pass] 240. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160614
|        [Pass] 241. Pass W20131119-160614-3 (10 sec)
|        [Pass] 241. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160614-3
|        [Pass] 254. Pass W20131119-160622-1 (2 sec)
|        [Pass] 254. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160622-1
|        [Pass] 191. Pass W20131119-160058-2 (327 sec)
|        [Pass] 191. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160058-2
|        [Pass] 245. Pass W20131119-160617-3 (9 sec)
|        [Pass] 245. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160617-3
|        [Pass] 248. Pass W20131119-160619-4 (7 sec)
|        [Pass] 248. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160619-4
|        [Pass] 249. Pass W20131119-160619-3 (9 sec)
|        [Pass] 249. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160619-3
|        [Pass] 250. Pass W20131119-160620 (10 sec)
|        [Pass] 250. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160620
|        [Pass] 252. Pass W20131119-160620-3 (10 sec)
|        [Pass] 252. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160620-3
|        [Pass] 253. Pass W20131119-160622 (8 sec)
|        [Pass] 253. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160622
|        [Pass] 255. Pass W20131119-160623 (8 sec)
|        [Pass] 255. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160623
|        [Pass] 256. Pass W20131119-160623-1 (9 sec)
|        [Pass] 256. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160623-1
|        [Pass] 257. Pass W20131119-160624 (9 sec)
|        [Pass] 257. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160624
|        [Pass] 213. Pass W20131119-160138-4 (305 sec)
|        [Pass] 213. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-160138-4
|        [Pass] 127. Pass W20131119-155918 (462 sec)
|        [Pass] 127. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-155918
|        [Pass] 100. Pass W20131119-155713 (600 sec)
|        [Pass] 100. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-155713
|        [Action]
|        [Action]
|         Results
|         `-------------------------------------------------`
|         Passing: 257
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /home/ati/HPCCSystems-regression/log/thor.13-11-19-15-55-32.log
|         `-------------------------------------------------`
|         Elapsed time: 701 sec  (00:11:41)
|         `-------------------------------------------------`
|

The logfile generated into the HPCCSystems-regression/log subfolder of the user personal folder and sorted by the test case number.


5. To run Regression Suite with selected test case on a selected cluster (e.g. Thor): 
-------------------------------------------------------------------------------------

(In this use case the default cluster is: thor)

Command:

        ./regress query [-h] [--publish] test_name [target cluster | all]

Positional arguments:
        test_name               Name of a single ECL query. It can contain wildcards. (mandatory).
        target cluster | all    Cluster for single query run (default: thor).
                                If cluster = 'all' then run ECL query on all clusters.
Optional arguments:
        -h, --help            Show help message and exit
        --publish             Publish compiled query instead of run.


The format of result is same as above:

6. Tags used in testcases:
--------------------------

    To exclude testcase from cluster or clusters, the tag is:
//no<cluster_name>

    To skip (similar to exclusion)
//skip type==<cluster> <reason>

    To build and publish testcase (e.g.:for libraries)
//publish

    To set individual timeout for test case
//timeout <timeout_value_in_sec>

    To switch off the test case output matching with key file
    (If this tag exists in the test case source then its output stored into the result log file.)
//nokey

    If //nokey is present then the following tag prevents the output being stored in the result log file.
//nooutput

7. Key file handling:
---------------------

After an ECL test case execution finished and all output collected the result checking follows these steps:

If the ECL source contains //nokey tag
    then the key file and output comparison skipped and the output can control by //nooutput tag
    else RS checks cluster specific key directory and key file existence
        If both exist
            then output compared with cluster specific keyfile
            else output compared with the keyfile located KEY directory

Examples:

We have a simple structure only one ECL file and two related keyfile. One in hthor and one in key directory.

 ecl
   |---hthor
   |     alljoin.xml
   |---key
   |     alljoin.xml
   |---setup
   alljoin.ecl

If we execute this query:

     ./regress query alljoin.ecl all

Then the RS executes alljoin.ecl on all target clusters and
    on hthor the output compared with hthor/alljoin.xml
    on thor and roxie the output compared with key/alljoin.xml


8. Key file generation:
-----------------------

The regression suite stores every test case output into ~/HPCCSystems-regression/result directory. This is the latest version of result. (The previous version can be found in ~/HPCCSystems-regression/archives directory.) When a test case execution finished Regression Suite compares this output file with the relevant key file to verify the result.

So if you have a new test case and it works well on all clusters (or some of them and excluded from all others by //no<cluster> tag inside it See: 6. ) then you can get key file in 2 steps:

1. Run test case with ./regress [suitedir] query <testcase.ecl> <cluster> .

2. Copy the output (testcase.xml) file from ~/HPCCSystems-regression/result to the relevant key file directory.

(To check everything is fine, repeat the step 1 and the query should now pass. )


9. Configuration setting in regress.json file:
-------------------------------------------------------------

        "ip": "127.0.0.1",                              - ECl server address
        "username": "regress",                          - Regression Suite dedicated username and pasword
        "password": "regress",
        "roxie": "127.0.0.1:9876",                      - Roxie server addres (not used)
        "server": "127.0.0.1:8010",                     - EclWatch service server address
        "suiteDir": "",                                 - default suite directory location - ""-> current directory
        "eclDir": "ecl",                                - ECL test cases directory source
        "setupDir": "ecl/setup",                        - ECL setup source directory
        "keyDir": "ecl/key",                            - XML key files directory to check testcases result
        "archiveDir": "archives",                       - Archive directory path for testcases generated XML results
        "resultDir": "results",                         - Current testcases generated XML results
        "regressionDir": "~/HPCCSystems-regression",    - Regression suite work and log file directory (in user private space)
        "logDir": "~/HPCCSystems-regression/log",       - Regression suite run log directory
        "Clusters": [                                   - List of known clusters name
            "hthor",
            "thor",
            "roxie"
        ],
        "timeout":"600",                                - Default test case timeout in sec. Can be override by command line parameter or //timeout tag in ECL file
        "maxAttemptCount":"3"                           - Max retry count to reset timeout if a testcase in any early stage (compiled, blocked) of execution pipeline.


10. Authentication:
-------------------

If your HPCC System is configured to use LDAP authentication you should change value of "username" and "password" fields in regress.json file to yours.

Alternatively, ensure that your test system has a user "regress" with password "regress" and appropriate rights to be able to run the suite.


Misc.
-----

**Important! Actually regression suite compares the test case result with xml files stored in testing/regression/ecl/key independently from the cluster.**
