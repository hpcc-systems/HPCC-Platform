Overview of Regression Suite usage
==================================

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
|        Results
|            `-------------------------------------------------`
|            Passing: 3
|            Failure: 0
|            `-------------------------------------------------`
|            Log: /var/log/HPCCSystems/regression/setup.13-06-17-09-50.log
|            `-------------------------------------------------`


	    

4. To run Regression Suite on a selected cluster (e.g. Thor) the suiteDir should point to absolute path of HPCC-Platform/testing/ directory:
--------------------------------------------------------------------------------------------------------------------------------------------
Command:

        ./regress --suiteDir $(HPCC_INS_DIR)/HPCC-Platform/testing run thor


The result is a long list of test cases (actually 482) and their result. 

(Actually the whole regression suite runs approximately 3 hours and generates full log in /var/log/HPCCSystems/regression directory. )

The first couple of lines look like this:

|
|        [Action] Suite: thor
|        [Action] Queries: 482
|        [Action] 1. Test: stepping7d.ecl
|        [Failure] Fail W20130617-111135
|        [Failure] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111135`
|        [Action] 2. Test: dbz2c.ecl
|        [Failure] Fail W20130617-111136
|        [Failure] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111136`
|        [Action] 3. Test: stepjoin2.ecl
|        [Pass] Pass W20130617-111137
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111137`
|        [Action] 4. Test: groupglobal3b.ecl
|        [Pass] Pass W20130617-111142
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111142`
|        [Action] 5. Test: realround.ecl
|        [Pass] Pass W20130617-111145
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111145`
|        [Action] 6. Test: patmin.ecl
|        [Pass] Pass W20130617-111146
|        [Pass] URL `http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130617-111146`
|        ...

After all test cases finished a short summary logged:

|
|       Results
|       `-------------------------------------------------`
|       Passing: 273
|       Failure: 209
|       `-------------------------------------------------`
|

It is followed by error list created by diff command between the test case generated xml output and the stored key xml files like this:

|
|        `---` stepping7d.xml
|        `+++` stepping7d.xml
|        @@ -1 +1,7 @@
|        -<Exception><Source>eclagent</Source><Message>Error: 0: </Message></Exception>
|        +<Dataset name='Result 1'>
|        +<Row><src>1</src><doc>5952</doc><cnt>7</cnt></Row>
|        +<Row><src>1</src><doc>5978</doc><cnt>16</cnt></Row>
|        +<Row><src>1</src><doc>26929</doc><cnt>3</cnt></Row>
|        +<Row><src>1</src><doc>27753</doc><cnt>9</cnt></Row>
|        +<Row><src>1</src><doc>34964</doc><cnt>4</cnt></Row>
|        +</Dataset>
|   
|        `---` dbz2c.xml
|
|        +++ dbz2c.xml
|      
|        @@ -1 +1 @@
|     
|        -<Exception><Source>eclagent</Source><Message>System error: -1: Division by  zero</Message></Exception>
|        +<Error><source>eclagent</source><code>-1</code><message>System error: -1:  Division by zero</message></Error>
|        
|        ...



5. To run Regression Suite with selected test case on a selected cluster (e.g. Thor): 
-------------------------------------------------------------------------------------

The --suiteDir should point to absolute path of HPCC-Platform/testing/ directory:

Command:

        ./regress --suiteDir $(HPCC_INST_DIR)/HPCC-Platform/testing query test_name cluster

E.g.run assert.ecl on thor cluster:

        ./regress --suiteDir $(HPCC_INST_DIR)/HPCC-Platform/testing query assert.ecl thor


The result is:

| 
|        [Action] 1. Test: assert.ecl
|        [Pass] Pass W20130618-133409
|        [Pass] URL http://127.0.0.1:8010/WsWorkunits/WUInfo?Wuid=W20130618-133409
|        [Action] 
|            Results
|            `-------------------------------------------------`
|            Passing: 1
|            Failure: 0
|            `-------------------------------------------------`
|            Log: /var/log/HPCCSystems/regression/assert.ecl.13-06-18-13-34.log
|            `-------------------------------------------------`
|

**Important! Actually regression suite always compares the test case result with xml files stored in testing/ecl/key independently from the cluster.**
