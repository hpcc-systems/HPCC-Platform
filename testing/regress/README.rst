Overview of Regression Suite usage
==================================
To use Regression Suite change directory to HPCC-Platform/testing/regress subdirectory.

Parameters of Regression Suite:
-------------------------------

Command:
 
 ./regress --help

Result:

|
|       usage: regress [-h] [--version] [--config [CONFIG]]
|                       [--loglevel [{info,debug}]] [--suiteDir [SUITEDIR]]
|                       {list,run,query} ...
| 
|       HPCC Platform Regression suite
| 
|       positional arguments:
|          {list,run}            sub-command help
|            list                list help
|            run                 run help
|            query               query help
|
|       optional arguments:
|            -h, --help            show this help message and exit
|            --version, -v         show program's version number and exit
|            --config [CONFIG]     Config file to use.
|            --loglevel [{info,debug}]
|                                  Set the log level.
|            --suiteDir [SUITEDIR], -s [SUITEDIR]
|                               suiteDir to use. Default value is the current directory and it can handle relative path.

	
Steps to run Regression Suite
=============================

1. Change directory to HPCC-Platform/testing/regress subdirectory.
------------------------------------------------------------------

2. To list all available clusters:
----------------------------------
Command:

    ./regress --suiteDir . list

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

        ./regress run thor


The result is a list of test cases and their result. 

The first and last couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 266
|        [Action] 1. Test: stepjoin2.ecl
|        [Pass] Pass W20131018-121854
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131018-121854
|        [Action] 2. Test: groupglobal3b.ecl
|        [Pass] Pass W20131018-121859
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131018-121859
|        [Action] 3. Test: realround.ecl
|        ...
|        [Action] 266. Test: lookupjoin.ecl
|        [Pass] Pass W20131018-124534
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131018-124534
|
|         Results
|         `-------------------------------------------------`
|         Passing: 266
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /var/log/HPCCSystems/regression/thor.13-10-18-12-18.log
|         `-------------------------------------------------`
|



5. Exclusions:
--------------
The exclusion.xml file can be used to exclude execution test case(s) on one or more clusters.

This example shows some examples: 

<exclusion>
    <cluster name="thor">
    </cluster>
    <cluster name="hthor">
        <exclude> platform.ecl </exclude>
    </cluster>
    <cluster name="roxie">
        <exclude> schedule1.ecl </exclude>
        <exclude> platform.ecl </exclude>
     </cluster>
</exclusion>

The platform.ecl excludes on hthor and roxie clusters, because it is thor specific. 
Actually roxie cluster doesn't support scheduling therefore schedule1.ecl excluded from this cluster.


6. To run Regression Suite with selected test case on a selected cluster (e.g. Thor): 
-------------------------------------------------------------------------------------

(In this use case the default cluster is: thor)

Command:

        ./regress query test_name cluster

or (for execute on thor)

        ./regress query test_name 

The result is same as above:

|
|        [Action] 1. Test: unicodelib.ecl
|        [Pass] Pass W20131018-134023
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131018-134023
|        [Action] 
|            Results
|            -------------------------------------------------
|            Passing: 1
|            Failure: 0
|            -------------------------------------------------
|            Log: /var/log/HPCCSystems/regression/unicodelib.ecl.13-10-18-13-40.log
|            -------------------------------------------------
|    

**Important! Actually regression suite always compares the test case result with xml files stored in testing/regression/ecl/key independently from the cluster.**

