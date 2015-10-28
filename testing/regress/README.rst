Overview of Regression Suite usage
==================================

To use Regression Suite change directory to HPCC-Platform/testing/regress subdirectory.

Regression Suite requires Python environment version >=2.6.6 and < 3.x

Global parameters of Regression Suite:
--------------------------------------

Command:
 
    ./ecl-test <-h|--help>

Result:

|
|       usage: ecl-test [-h] [--config [CONFIG]]
|                       [--loglevel [{info,debug}]]
|                       [--suiteDir [SUITEDIR]]
|                       [--timeout [TIMEOUT]]
|                       [--keyDir [KEYDIR]]
|                       [--ignoreResult]
|                       [-X name1=value1[,name2=value2...]]
|                       [-f optionA=valueA[,optionB=valueB...]]
|                       [--pq threadNumber]
|                       [--noversion]
|                       [--runclass class[,class,...]]
|                       [--excludeclass class[,class,...]]
|                       {list,setup,run,query} ...
| 
|       HPCC Platform Regression suite
| 
|       positional arguments:
|          {list,setup,run,query} sub-command help
|            list                 list help
|            setup                setup help
|            run                  run help
|            query                query help
|
|       optional arguments:
|        -h, --help               show this help message and exit
|        --config [CONFIG]        config file to use. Default: ecl-test.json.
|        --loglevel [{info,debug}]
|                                 set the log level. Use debug for more detailed logfile.
|        --suiteDir [SUITEDIR], -s [SUITEDIR]
|                                 suiteDir to use. Default value is the current directory and it can handle relative path.
|        --timeout [TIMEOUT]      timeout for query execution in sec. Use -1 to disable timeout. Default value defined in ecl-test.json config file (see: 9.)
|        --keyDir [KEYDIR], -k [KEYDIR]
|                                 key file directory to compare test output. Default value defined in regress.json config file.
|        --ignoreResult, -i       completely ignore the result.
|        -X name1=value1[,name2=value2...]
|                                 sets the stored input value (stored('name')).
|        -f optionA=valueA[,optionB=valueB...]
|                                 set an ECL option (equivalent to #option).
|        --pq threadNumber        parallel query execution with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumber = numberOfLocalCore * 2)
|        --noversion              avoid version expansion of queries. Execute them as a standard test.
|        --runclass class[,class,...], -r class[,class,...]
|                                 run subclass(es) of the suite. Default value is 'all'
|        --excludeclass class[,class,...], -e class[,class,...]
|                                 exclude subclass(es) of the suite. Default value is 'none'
|

Important!
    There is a bug in Python argparse library whichis impacts the quoted parameters. So either in -X or -f or both contains a value with space(s) inside then the whole argument should be put in double quote!

    Example: We should pass these names values pairs to set stored input values:
                param1 = 1
                param2 = A string
                param2 = Other string

    The proper ecl-test command is:
            ./ecl-test -X"param1=1,param2=A string,param3=Other String" ...

    Same format should use for -f option(s) and values. This problem doesn't impact parameters are stored in ecl-test.json config file. (See 9.)


Parameters of Regression Suite list sub-command:
------------------------------------------------

Command:

    ./ecl-test list <-h|--help>

Result:

|
|       usage: ecl-test list [-h] [--config [CONFIG]]
|                            [--loglevel [{info,debug}]]
|
|       positional arguments:
|        targets                  print target clusters from config (ecl-test.json by default).
|
|       optional arguments:
|        -h, --help               show this help message and exit
|        --config [CONFIG]        config file to use. Default: ecl-test.json
|        --loglevel [{info,debug}]
|                                 set the log level. Use debug for more detailed logfile.
|

Parameters of Regression Suite setup sub-command:
-------------------------------------------------

Command:

    ./ecl-test setup <-h|--help>

Result:

|
|       usage: ecl-test setup [-h] [--config [CONFIG]]
|                             [--loglevel [{info,debug}]]
|                             [--suiteDir [SUITEDIR]]
|                             [--timeout [TIMEOUT]]
|                             [--keyDir [KEYDIR]]
|                             [--ignoreResult]
|                             [-X name1=value1[,name2=value2...]]
|                             [-f optionA=valueA[,optionB=valueB...]]
|                             [--pq threadNumber]
|                             [--noversion]
|                             [--runclass class[,class,...]]
|                             [--excludeclass class[,class,...]]
|                             [--target [target_cluster_list | all]]
|
|       optional arguments:
|        -h, --help               show this help message and exit
|        --config [CONFIG]        config file to use. Default: ecl-test.json.
|        --loglevel [{info,debug}]
|                                 set the log level. Use debug for more detailed logfile.
|        --suiteDir [SUITEDIR], -s [SUITEDIR]
|                                 suiteDir to use. Default value is the current directory and it can handle relative path.
|        --timeout [TIMEOUT]      timeout for query execution in sec. Use -1 to disable timeout. Default value defined in ecl-test.json config file (see: 9.)
|        --keyDir [KEYDIR], -k [KEYDIR]
|                                 key file directory to compare test output. Default value defined in regress.json config file.
|        --ignoreResult, -i       completely ignore the result.
|        -X name1=value1[,name2=value2...]
|                                 sets the stored input value (stored('name')).
|        -f optionA=valueA[,optionB=valueB...]
|                                 set an ECL option (equivalent to #option).
|        --pq threadNumber        parallel query execution with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumber = numberOfLocalCore * 2)
|        --noversion              avoid version expansion of queries. Execute them as a standard test.
|        --runclass class[,class,...], -r class[,class,...]
|                                 run subclass(es) of the suite. Default value is 'all'
|        --excludeclass class[,class,...], -e class[,class,...]
|                                 exclude subclass(es) of the suite. Default value is 'none'
|        --target [target_cluster_list | all], -t [target_cluster_list | all]
|                                 run the setup on target cluster(s). If target = 'all' then run setup on all clusters. If undefined the config 'defaultSetupClusters' value will be used.
|

Parameters of Regression Suite run sub-command:
-----------------------------------------------

Command:

    ./ecl-test run <-h|--help>

Result:

|
|       usage: ecl-test run [-h][--config [CONFIG]]
|                           [--loglevel [{info,debug}]]
|                           [--suiteDir [SUITEDIR]]
|                           [--timeout [TIMEOUT]]
|                           [--keyDir [KEYDIR]]
|                           [--ignoreResult]
|                           [-X name1=value1[,name2=value2...]]
|                           [-f optionA=valueA[,optionB=valueB...]]
|                           [--pq threadNumber]
|                           [--noversion]
|                           [--runclass class[,class,...]]
|                           [--excludeclass class[,class,...]]
|                           [--target [target_cluster_list | all]]
|                           [--publish]
|
|       optional arguments:
|        -h, --help               show this help message and exit
|        --config [CONFIG]        config file to use. Default: ecl-test.json.
|        --loglevel [{info,debug}]
|                                 set the log level. Use debug for more detailed logfile.
|        --suiteDir [SUITEDIR], -s [SUITEDIR]
|                                 suiteDir to use. Default value is the current directory and it can handle relative path.
|        --timeout [TIMEOUT]      timeout for query execution in sec. Use -1 to disable timeout. Default value defined in ecl-test.json config file (see: 9.)
|        --keyDir [KEYDIR], -k [KEYDIR]
|                                 key file directory to compare test output. Default value defined in regress.json config file.
|        --ignoreResult, -i       completely ignore the result.
|        -X name1=value1[,name2=value2...]
|                                 sets the stored input value (stored('name')).
|        -f optionA=valueA[,optionB=valueB...]
|                                 set an ECL option (equivalent to #option).
|        --pq threadNumber        parallel query execution with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumber = numberOfLocalCore * 2)
|        --noversion              avoid version expansion of queries. Execute them as a standard test.
|        --runclass class[,class,...], -r class[,class,...]
|                                 run subclass(es) of the suite. Default value is 'all'
|        --excludeclass class[,class,...], -e class[,class,...]
|                                 exclude subclass(es) of the suite. Default value is 'none'
|        --target [target_cluster_list | all], -t [target_cluster_list | all]
|                                 run the setup on target cluster(s). If target = 'all' then run setup on all clusters. If undefined the config 'defaultSetupClusters' value will be used.
|        --publish, -p            publish compiled query instead of run.
|


Parameters of Regression Suite query sub-command:
-------------------------------------------------

Command:

    ./ecl-test query <-h|--help>

Result:

|
|       usage: ecl-test query [-h] [--config [CONFIG]]
|                             [--loglevel [{info,debug}]]
|                             [--suiteDir [SUITEDIR]]
|                             [--timeout [TIMEOUT]]
|                             [--keyDir [KEYDIR]]
|                             [--ignoreResult]
|                             [-X name1=value1[,name2=value2...]]
|                             [-f optionA=valueA[,optionB=valueB...]]
|                             [--pq threadNumber]
|                             [--noversion]
|                             [--runclass class[,class,...]]
|                             [--excludeclass class[,class,...]]
|                             [--target [target_cluster_list | all]]
|                             [--publish]
|                             ECL_query [ECL_query ...]
|
|       positional arguments:
|        ECL_query                name of one or more ECL file(s). It can contain wildcards. (mandatory).
|
|       optional arguments:
|        -h, --help               show this help message and exit
|        --config [CONFIG]        config file to use. Default: ecl-test.json.
|        --loglevel [{info,debug}]
|                                 set the log level. Use debug for more detailed logfile.
|        --suiteDir [SUITEDIR], -s [SUITEDIR]
|                                 suiteDir to use. Default value is the current directory and it can handle relative path.
|        --timeout [TIMEOUT]      timeout for query execution in sec. Use -1 to disable timeout. Default value defined in ecl-test.json config file (see: 9.)
|        --keyDir [KEYDIR], -k [KEYDIR]
|                                 key file directory to compare test output. Default value defined in regress.json config file.
|        --ignoreResult, -i       completely ignore the result.
|        -X name1=value1[,name2=value2...]
|                                 sets the stored input value (stored('name')).
|        -f optionA=valueA[,optionB=valueB...]
|                                 set an ECL option (equivalent to #option).
|        --pq threadNumber        parallel query execution with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumber = numberOfLocalCore * 2)
|        --noversion              avoid version expansion of queries. Execute them as a standard test.
|        --runclass class[,class,...], -r class[,class,...]
|                                 run subclass(es) of the suite. Default value is 'all'
|        --excludeclass class[,class,...], -e class[,class,...]
|                                 exclude subclass(es) of the suite. Default value is 'none'
|        --target [target_cluster_list | all], -t [target_cluster_list | all]
|                                 run the setup on target cluster(s). If target = 'all' then run setup on all clusters. If undefined the config 'defaultSetupClusters' value will be used.
|        --publish, -p            publish compiled query instead of run.
|

Steps to run Regression Suite
=============================

1. Change directory to HPCC-Platform/testing/regress subdirectory.
------------------------------------------------------------------

2. To list all available clusters:
----------------------------------
Command:

    ./ecl-test list

The result looks like this:

        Available Clusters: 
            - hthor
            - thor
            - roxie



3. To run the Regression Suite setup:
-------------------------------------

Command:

        ./ecl-test setup

to run setup on the default (thor) cluster

or
        ./ecl-test setup -t <target cluster> | all

to run setup on a selected or all clusters

The result for thor:

|
|        [Action] Suite: thor (setup)
|        [Action] Queries: 4
|        [Action]   1. Test: setup.ecl
|        [Pass]   1. Pass W20140410-133419 (8 sec)
|        [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140410-133419
|        [Action]   2. Test: setup_fetch.ecl
|        [Pass]   2. Pass W20140410-133428 (3 sec)
|        [Pass]   2. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140410-133428
|        [Action]   3. Test: setupsq.ecl
|        [Pass]   3. Pass W20140410-133432 (5 sec)
|        [Pass]   3. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140410-133432
|        [Action]   4. Test: setupxml.ecl
|        [Pass]   4. Pass W20140410-133438 (2 sec)
|        [Pass]   4. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140410-133438
|        [Action]
|            Results
|            -------------------------------------------------
|            Passing: 4
|            Failure: 0
|            -------------------------------------------------
|            Log: /home/ati/HPCCSystems-regression/log/thor.14-04-10-13-34-18.log
|            -------------------------------------------------
|            Elapsed time: 24 sec  (00:00:24)
|            -------------------------------------------------
|

To setup the proper environment for text search test cases there is a new component called setuptext.ecl. It uses data files from another location and the default location stored into the options.ecl. RS generates location from the run-time environment and passes it to the setup via stored variable called 'OriginalTextFilesEclPath'.

4. To run Regression Suite on a selected cluster (e.g. Thor):
-------------------------------------------------------------
Command:

        ./ecl-test run [-t <target cluster>|all] [-h] [--pq threadNumber]

Optional arguments:
  -h, --help         show help message and exit
   --target [target_cluster | all], -t [target_cluster | all]
|                        Target cluster for single query run. If target = 'all' then run query on all clusters. Default value is thor.
  --pq threadNumber  Parallel query execution with threadNumber threads.
                    ('-1' can be use to calculate usable thread count on a single node system)

The result is a list of test cases and their result. 

The first and last couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 320
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
|        [Action] 319. Test: xmlout2.ecl
|        [Pass] Pass W20131119-182536 (1 sec)
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-182536
|        [Action] 320. Test: xmlparse.ecl
|        [Pass] Pass W20131119-182537 (1 sec)
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131119-182537
|
|         Results
|         `-------------------------------------------------`
|         Passing: 320
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /home/ati/HPCCSystems-regression/log/thor.13-11-19-17-52-27.log
|         `-------------------------------------------------`
|         Elapsed time: 2367 sec  (00:39:27)
|         `-------------------------------------------------`
|

If --pq option used (in this case with 16 threads) then then the content of the console log will be different like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 320
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
|         Passing: 320
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /home/ati/HPCCSystems-regression/log/thor.14-04-10-16-12-30.log
|         `-------------------------------------------------`
|         Elapsed time: 1498 sec  (00:24:58)
|         `-------------------------------------------------`
|

The logfile generated into the HPCCSystems-regression/log subfolder of the user personal folder and sorted by the test case number.


5. To run Regression Suite with selected test case on a selected cluster (e.g. Thor) or all:
--------------------------------------------------------------------------------------------------------------------------

Command:

        ./ecl-test query test_name [test_name...] [-h] [--target <cluster|all>] [--publish] [--pq <threadNumber|-1>]

Positional arguments:
        test_name               Name of a single ECL query. It can contain wildcards. (mandatory).

Optional arguments:
        -h, --help            Show help message and exit
        --target [target_cluster | all], -t [target_cluster | all]
                              Target cluster for query to run. If target = 'all' then run query on all clusters. Default value is thor.
        --publish             Publish compiled query instead of run.
        --pq threadNumber     Parallel query execution for multiple test cases specified in CLI with threadNumber threads. (If threadNumber is '-1' on a single node system then threadNumer = numberOfLocalCore * 2 )



The format of the output is the same as 'run', except there is a log, result and diff per cluster targeted:

|         [Action] Suite: hthor
|         [Action] Queries: 9
|         [Action]
|         [Action]   1. Test: aggsq1.ecl
|         [Action]   2. Test: aggsq1a.ecl
|         [Action]   3. Test: aggsq1seq.ecl
|         [Pass]   1. Pass W20140313-171024 (2 sec)
|         [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171024
|         [Action]   4. Test: aggsq2.ecl
|         [Action]   5. Test: aggsq2seq.ecl
|         [Failure]   2. Fail W20140313-171025 (2 sec)
|         [Failure]   2. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171025
|         [Action]   6. Test: aggsq3.ecl
|         [Pass]   3. Pass W20140313-171026 (2 sec)
|         [Pass]   3. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171026
|         [Action]   7. Test: aggsq3seq.ecl
|         [Pass]   4. Pass W20140313-171027 (2 sec)
|         [Pass]   4. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171027
|         [Action]   8. Test: aggsq4.ecl
|         [Pass]   5. Pass W20140313-171028 (2 sec)
|         [Pass]   5. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171028
|         [Action]   9. Test: aggsq4seq.ecl
|         [Pass]   6. Pass W20140313-171029 (2 sec)
|         [Pass]   6. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171029
|         [Pass]   7. Pass W20140313-171029-1 (3 sec)
|         [Pass]   7. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171029-1
|         [Pass]   8. Pass W20140313-171030 (2 sec)
|         [Pass]   8. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171030
|         [Pass]   9. Pass W20140313-171031 (2 sec)
|         [Pass]   9. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171031
|         [Action]
|         [Action]
|             Results
|             `-------------------------------------------------`
|             Passing: 8
|             Failure: 1
|             `-------------------------------------------------`
|             KEY FILE NOT FOUND. /home/ati/MyPython/RegressionSuite/ecl/key/aggsq1a.xml
|             `-------------------------------------------------`
|             Log: /home/ati/HPCCSystems-regression/log/hthor.14-03-13-17-10-24.log
|             `-------------------------------------------------`
|             Elapsed time: 10 sec  (00:00:10)
|             `-------------------------------------------------`
|
|         [Action] Suite: thor
|         [Action] Queries: 2
|         [Action]
|         [Action]   1. Test: aggsq2.ecl
|         [Action]   2. Test: aggsq2seq.ecl
|         [Pass]   1. Pass W20140313-171035 (3 sec)
|         [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171035
|         [Pass]   2. Pass W20140313-171036 (4 sec)
|         [Pass]   2. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140313-171036
|         [Action]
|         [Action]
|             Results
|             `-------------------------------------------------`
|             Passing: 2
|             Failure: 0
|             `-------------------------------------------------`
|             Log: /home/ati/HPCCSystems-regression/log/thor.14-03-13-17-10-35.log
|             `-------------------------------------------------`
|             Elapsed time: 7 sec  (00:00:07)
|             `-------------------------------------------------`
|
|         [Action] Suite: roxie
|         [Action] Queries: 0
|         [Action]
|         [Action]
|         [Action]
|             Results
|             `-------------------------------------------------`
|             Passing: 0
|             Failure: 0
|             `-------------------------------------------------`
|             Log: /home/ati/HPCCSystems-regression/log/roxie.14-03-13-17-10-42.log
|             `-------------------------------------------------`
|             Elapsed time: 2 sec  (00:00:02)
|             `-------------------------------------------------`
|
|         End.


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

    To define a class to be executed/excluded in run mode.
//class=<class_name>

    To allow multiple tests to be generated from a single source file
    The regression suite engine executes the file once for each //version line in the file. It is compiled with command line option -Dn1=v1 -Dn2=v2 etc.
    The string value should quoted with \'.
    Optionally 'no<target>' exclusion info can add at the end of tag.
//version <n1>=<v1>,<n2>=<v2>,...[,no<target>[,no<target>]]

    This tag should use when a test case intentionally fails to handle it as pass.
    If a test case intentionally fails then it should fail on all allowed platforms.
//fail


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

For Setup keyfile handling same as Run/Query except the target specific keyfile stored platform directory under setup:

ecl
   |---hthor
   |     alljoin.xml
   |---key
   |     alljoin.xml
   |     setup.xml
   |     setup_fetch.xml
   |     setup_sq.xml
   |     setup_xml.xml
   |---setup
   |      |
   |      ---hthor
   |      |       setup.xml
   |      setup.ecl
   |      setup_fetch.ecl
   |      setup_sq.ecl
   |      setup_xml.ecl
   alljoin.ecl|

If we execute setup on target hthor:

     ./regress  setup -t hthor

Then the RS executes all ecl files from setup directory and 
    - the result of setup.ecl compared with ecl/setup/hthor/setup.xml
    - all other test cases results compared with corresponding file in ecl/key directory.

If we execute setup on any other target:

     ./regress  setup -t thor|roxie

Then the RS executes all ecl files from setup directory and 
    - the test cases results compared with corresponding file in ecl/key directory.

8. Key file generation:
-----------------------

The regression suite stores every test case output into ~/HPCCSystems-regression/result directory. This is the latest version of result. (The previous version can be found in ~/HPCCSystems-regression/archives directory.) When a test case execution finished Regression Suite compares this output file with the relevant key file to verify the result.

So if you have a new test case and it works well on all clusters (or some of them and excluded from all others by //no<cluster> tag inside it See: 6. ) then you can get key file in 2 steps:

1. Run test case with ./ecl-test [suitedir] query <testcase.ecl> <cluster> .

2. Copy the output (testcase.xml) file from ~/HPCCSystems-regression/result to the relevant key file directory.

(To check everything is fine, repeat the step 1 and the query should now pass. )

9. Configuration setting in ecl-test.json file:
-------------------------------------------------------------

        "roxieTestSocket": ":9876",                     - Roxie test socket address (not used)
        "espIp": "127.0.0.1",                           - ESP server IP
        "espSocket": ":8010",                           - ESP service address
        "username": "regress",                          - Regression Suite dedicated username and pasword
        "password": "regress",
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
        "timeout":"720",                                - Default test case timeout in sec. Can be override by command line parameter or //timeout tag in ECL file
        "maxAttemptCount":"3"                           - Max retry count to reset timeout if a testcase in any early stage (compiled, blocked) of execution pipeline.

Optionally the config file can contain some sections of default values:

If the -t | --target command line parameter is omitted then the regression test engine uses the default target(s) from one of these default definitions. If undefined, then the engine uses the first cluster from the Cluster array.

        "defaultSetupClusters": [
            "hthor",
            "thor3"
        ]

        "defaultTargetClusters": [
            "thor",
            "thor3"
        ]

For stored parameters:

    "Params":[
                "querya.ecl:param1=value1,param2=value2",
                "queryb.ecl:param1=value3",
                "some*.ecl:paramforsome=value4",
                "*.ecl:globalparam=blah"
            ]

The Regression Suite processes the Params definition(s) sequentially. The -Xname=value command line parameter overrides any values defined in this section.
Examples:

We have an ECL source called PassTest.ecl with these lines:

|    //nokey        # To avoid result comparison error
|    string bla := 'EN' : STORED('bla');
|    output(bla);

1. For the purposes of this example, we assume there is no Params section in the testing/regress/ecl_test.json file or it is empty and there are no PassTest.ecl related global entries.

If we execute it with query mode:

|     ./ecl_test query PassTest.ecl -t hthor

The result is:

|     [Action] Target: hthor
|     [Action] Queries: 1
|     [Action]   1. Test: PassTest.ecl
|     [Pass]   1. Pass W20140508-180241 (1 sec)
|     [Pass]   1. URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20140508-180241
|     [Action]
|         Results
|         -------------------------------------------------
|         Passing: 1
|         Failure: 0
|         -------------------------------------------------
|         u"Output of PassTest.ecl test is:\n\t<Dataset name='Result 1'>\n <Row><Result_1>EN</Result_1></Row>\n</Dataset>\n"
|         -------------------------------------------------
|         Log: /home/ati/HPCCSystems-regression/log/hthor.14-05-08-18-02-41.log
|         -------------------------------------------------
|         Elapsed time: 4 sec  (00:00:04)
|         -------------------------------------------------

2. Same as 1. but execute it in query mode with -X parameter:

|     ./ecl_test -Xbla=blabla query PassTest.ecl -t hthor

then the output of PassTest.ecl changes in the result:
|         -------------------------------------------------
|         u"Output of PassTest.ecl test is:\n\t<Dataset name='Result 1'>\n <Row><Result_1>blabla</Result_1></Row>\n</Dataset>\n"
|         -------------------------------------------------

3. If we want to apply same stored value every execution then we can put it into the ecl_test.json configuration file:

|    "Params":[
|                "PassTest.ecl:bla='A value'"
|          ]

We can execute it with a simple query mode:

|     ./ecl_test query PassTest.ecl -t hthor

then the output of PassTest.ecl changes in the result accordingly with the value from the Params option:
|         -------------------------------------------------
|         u"Output of PassTest.ecl test is:\n\t<Dataset name='Result 1'>\n <Row><Result_1>A value</Result_1></Row>\n</Dataset>\n"
|         -------------------------------------------------

4. Finally we have value(s) in the config file, but we want to run PassTest.ecl with another input value.

In this case we can use same command as in 2. with a new value:

|     ./ecl_test -Xbla='Another value' query PassTest.ecl -t hthor

then the output of PassTest.ecl changes in the result:
|         -------------------------------------------------
|         u"Output of PassTest.ecl test is:\n\t<Dataset name='Result 1'>\n <Row><Result_1>Another value</Result_1></Row>\n</Dataset>\n"
|         -------------------------------------------------

We can use as many values as we need in this form:
|       -Xname1=value1,name2=value2...

Important!
    There should not be any spaces before or after the commas.
    If there is more than one -X in the command line, the last will be the active and all other discarded.



10. Authentication:
-------------------

If your HPCC System is configured to use LDAP authentication you should change value of "username" and "password" fields in ecl-test.json file to yours.

Alternatively, ensure that your test system has a user "regress" with password "regress" and appropriate rights to be able to run the suite.

