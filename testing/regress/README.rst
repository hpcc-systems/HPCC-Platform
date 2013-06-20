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
|                               suiteDir to use. Except the list it should be an absolute path!!

	
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



3. The first item of the list (setup) is the default target, but its need to suiteDir set to HPCC-Platform/testing/regress directory! 
-------------------------------------------------------------------------------------------------------------------------------------

Actually there are two reasons for this:
	1. To point ecl files of new regression suite located testing/regress/ecl/setup directory
	2. To point matched key files.)

Command:

        ./regress --suiteDir $(HPCC_INST_DIR)/HPCC-Platform/testing/regress run

or

        ./regress --suiteDir $(HPCC_INS_DIR)/HPCC-Platform/testing/regress run setup

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


	    

4. To run Regression Suite on a selected cluster (e.g. Thor) the suiteDir should point to absolute path of HPCC-Platform/testing/ directory:
--------------------------------------------------------------------------------------------------------------------------------------------
Command:

        ./regress --suiteDir $(HPCC_INS_DIR)/HPCC-Platform/testing/regress run thor


The result is a list of test cases (actually 0) and their result. 

The first couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 0
|        [Action] 
|         Results
|         `-------------------------------------------------`
|         Passing: 0
|         Failure: 0
|         `-------------------------------------------------`
|         Log: /var/log/HPCCSystems/regression/thor.13-06-20-13-40.log
|         `-------------------------------------------------`
|


5. To run Regression Suite with selected test case on a selected cluster (e.g. Thor): 
-------------------------------------------------------------------------------------

The --suiteDir should point to absolute path of HPCC-Platform/testing/regress directory:

Command:

        ./regress --suiteDir $(HPCC_INS_DIR)/HPCC-Platform/testing/regress query test_name cluster


Actually the result is an error message "[Errno 2] No such file or directory: 'path'" related we have no test case in the Regression Suite.

**Important! Actually regression suite always compares the test case result with xml files stored in testing/regression/ecl/key independently from the cluster.**
