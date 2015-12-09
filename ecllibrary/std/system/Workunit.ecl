/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

import lib_workunitservices;

RETURN MODULE

EXPORT WorkunitRecord := lib_workunitservices.WsWorkunitRecord;

EXPORT TimeStampRecord := lib_workunitservices.WsTimeStamp;

EXPORT MessageRecord := lib_workunitservices.WsMessage;

EXPORT FileReadRecord := lib_workunitservices.WsFileRead;

EXPORT FileWrittenRecord := lib_workunitservices.WsFileWritten;

EXPORT TimingRecord := lib_workunitservices.WsTiming;

/**
The statistic record exported from the plugin has the following format:

WsStatistic := RECORD
    unsigned8 value;
    unsigned8 count;
    unsigned8 maxValue;
    string creatorType;
    string creator;
    string scopeType;
    string scope;
    string name;
    string description;
    string unit;
END;
*/

EXPORT StatisticRecord := lib_workunitservices.WsStatistic;

/*
 * Returns a Boolean indication whether the work unit exists.
 * 
 * @param wuid          A string containing the WorkUnit IDentifier to locate.
 * @param online        the flag specifying whether the search is performed online.
 * @param archived      the flag specifying whether the search is performed in the archives.
 */

EXPORT BOOLEAN WorkunitExists(varstring wuid, boolean online=true, boolean archived=false) :=
    lib_workunitservices.WorkUnitServices.WorkunitExists(wuid, online, archived);

/*
 * Returns a dataset of all workunits that meet the search criteria.
 *
 * @param lowwuid       the lowest WorkUnit IDentifier to list. This may be an empty string.
 * @param highwuid      the highest WorkUnit IDentifier to list.
 * @param cluster       the name of the cluster the workunit ran on.
 * @param jobname       the name of the workunit. This may contain wildcard ( * ? ) characters.
 * @param state         the state of the workunit.
 * @param priority      a string containing the priority of the workunit.
 * @param fileread      the name of a file read by the workunit. This may contain wildcard ( * ? ) characters.
 * @param filewritten   the name of a file written by the workunit. This may contain wildcard ( * ? ) characters.
 * @param roxiecluster  the name of the Roxie cluster.
 * @param eclcontains   the text to search for in the workunit�s ECL code. This may contain wildcard ( * ? ) characters.
 * @param online        the flag specifying whether the search is performed online.
 * @param archived      the flag specifying whether the search is performed in the archives.
 * @param appvalues     application values to search for. Use a string of the form appname/key=value or appname/*=value.
 */

EXPORT dataset(WorkunitRecord) WorkunitList(
                                         varstring lowwuid='',
                                         varstring highwuid='', 
                                         varstring username='', 
                                         varstring cluster='', 
                                         varstring jobname='',
                                         varstring state='', 
                                         varstring priority='',
                                         varstring fileread='',
                                         varstring filewritten='',
                                         varstring roxiecluster='',
                                         varstring eclcontains='',
                                         boolean online=true,
                                         boolean archived=false,
                                         varstring appvalues=''
                                        ) :=
    lib_workunitservices.WorkUnitServices.WorkunitList(
                                        lowwuid, highwuid, 
                                        username, cluster, jobname, state, priority,
                                        fileread, filewritten, roxiecluster, eclcontains,
                                        online, archived, appvalues);

/*
 * Returns a valid Workunit identifier for the specified date and time.  This is useful for creating ranges of wuids 
 * that can be passed to WorkunitList()
 *
 * @param year          the year
 * @param month         the month
 * @param day           the day
 * @param hour          the hour
 * @param minute        the minute
*/

EXPORT VARSTRING WUIDonDate(UNSIGNED4 year, UNSIGNED4 month, UNSIGNED4 day, UNSIGNED4 hour, UNSIGNED4 minute) :=
    lib_workunitservices.WorkUnitServices.WUIDonDate(year,month,day,hour,minute);

/*
 * Returns a valid Workunit identifier for a work unit that would be been created in the past.
 *
 * @param days_ago      the number of days in the past to create the wuid for.
*/

EXPORT VARSTRING WUIDdaysAgo(UNSIGNED4 days_ago) :=
  lib_workunitservices.WorkUnitServices.WUIDdaysAgo(days_ago); 

/*
 * Returns the timestamp information from a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(TimeStampRecord) WorkunitTimeStamps(varstring wuid) :=
  lib_workunitservices.WorkUnitServices.WorkunitTimeStamps(wuid); 

/*
 * Returns a dataset of the messages reported from a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(MessageRecord) WorkunitMessages(varstring wuid) :=
  lib_workunitservices.WorkUnitServices.WorkunitMessages(wuid); 

/*
 * Returns a dataset of the files that were read by a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(FileReadRecord) WorkunitFilesRead(varstring wuid) :=
  lib_workunitservices.WorkUnitServices.WorkunitFilesRead(wuid); 

/*
 * Returns a dataset of files that were written by a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(FileWrittenRecord) WorkunitFilesWritten(varstring wuid) :=
  lib_workunitservices.WorkUnitServices.WorkunitFilesWritten(wuid); 

/*
 * Returns the timing information from a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(TimingRecord) WorkunitTimings(varstring wuid) :=
  lib_workunitservices.WorkUnitServices.WorkunitTimings(wuid); 

/*
 * Returns the statistics from a particular workunit.
 *
 * @param wuid          the name of the workunit
*/

EXPORT dataset(StatisticRecord) WorkunitStatistics(varstring wuid, boolean includeActivities = false, varstring _filter = '') :=
  lib_workunitservices.WorkUnitServices.WorkunitStatistics(wuid, includeActivities, _filter);

/*
 * Sets an application value in current workunit. Returns true if the value was set successfully.
 *
 * @param app           the app name to set.
 * @param key           the name of the value to set.
 * @param value         the value to set.
 * @param overwrite     whether an existing value should be overwritten (default=true).
*/

EXPORT boolean SetWorkunitAppValue(varstring app, varstring key, varstring value, boolean overwrite=true) :=
  lib_workunitservices.WorkUnitServices.SetWorkunitAppValue(app, key, value, overwrite);


END;
