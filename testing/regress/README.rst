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

        ./regress run thor


The result is a list of test cases and their result. 

The first and last couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 253
|        [Action] 
|        [Action] 1. Test: groupglobal3b.ecl
|        [Pass] Pass W20131029-165658
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131029-165658
|        [Action] 2. Test: realround.ecl
|        [Pass] Pass W20131029-165701
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131029-165701
|        [Action] 3. Test: patmin.ecl
|        .
|        .
|        .
|        [Action] 252. Test: ds_map.ecl
|        [Pass] Pass W20131029-171831
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131029-171831
|        [Action] 253. Test: lookupjoin.ecl
|        [Pass] Pass W20131029-171833
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20131029-171833
|        
|         Results
|         `-------------------------------------------------`
|         Passing: 253
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /var/log/HPCCSystems/regression/thor.13-10-29-16-56.log
|         `-------------------------------------------------`
|


5. To run Regression Suite with selected test case on a selected cluster (e.g. Thor): 
-------------------------------------------------------------------------------------

(In this use case the default cluster is: thor)

Command:

        ./regress query test_name [cluster]


The format of result is same as above:

6. Tags used in testcases:
--------------------------

To exclude testcase from cluster or clusters, the tag is:
//no<cluster_name>

To skip (similar to exclusion)
//skip type==<cluster> <reason>

To build and publish testcase (e.g.:for libraries)
//publish

**Important! Actually regression suite compares the test case result with xml files stored in testing/regression/ecl/key independently from the cluster.**
