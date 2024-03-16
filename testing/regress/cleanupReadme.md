# Cleanup Parameter of Regression Suite run and query sub-command

The cleanup parameter has been introduced to allow the user to automatically delete the workunits created by executing the Regression Suite on their local system.

It is an optional argument of the run and query sub-command.

A custom logging system also creates log files for each execution of the run and query sub-command that contains information about the workunit deletion. 

### Command:
> ./ecl-test run --cleanup [mode]

> ./ecl-test query --cleanup [mode]

Modes allowed are ‘workunits’, ‘passed’. Default is ‘none’. 
 
- workunits - all passed and failed workunits are deleted.

- passed - only the passed workunits of the queries executed are deleted.  

- none - no workunits created during the current run command are deleted.  

### Result:
#### The sample terminal output for hthor target:

>./ecl-test query ECL_query action1.ecl action2.ecl action4.ecl action5.ecl -t hthor --cleanup workunits

[Action] Suite: hthor  
[Action] Queries: 4  
[Action]   1. Test: action1.ecl  
[Pass]   1. Pass action1.ecl - W20240526-094322 (2 sec)  
[Pass]   1. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240526-094322  
[Action]   2. Test: action2.ecl  
[Pass]   2. Pass action2.ecl - W20240526-094324 (2 sec)  
[Pass]   2. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240526-094324  
[Action]  3. Test: action4.ecl  
[Pass]  3. Pass action4.ecl - W20240526-094325 (2 sec)  
[Pass]  3. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240526-094325  
[Action]   4. Test: action5.ecl  
[Pass]   4. Pass action5.ecl - W20240526-094327 (2 sec)  
[Pass]   4. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240526-094327  
[Action]     
    -------------------------------------------------  
    Result:  
    Passing: 4     
    Failure: 0  
    -------------------------------------------------  
    Log: /root/HPCCSystems-regression/log/hthor.24-05-26-09-43-22.log  
    -------------------------------------------------  
    Elapsed time: 11 sec  (00:00:11)   
    -------------------------------------------------  

[Action] Automatic Cleanup Routine  
    
[Pass] 1. Workunit Wuid=W20240526-094322 deleted successfully.  
[Pass] 2. Workunit Wuid=W20240526-094324 deleted successfully.  
[Failure] 3. Failed to delete Wuid=W20240526-094325. URL: http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240526-094325  
            Failed cannot open workunit  Wuid=W20240526-094325.. Response status code: 200  
[Pass] 4. Workunit Wuid=W20240526-094327 deleted successfully.  
Suite destructor.  