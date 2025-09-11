/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

// Test will fail on default configuration because log4j redirection is disabled by default
// To test, set log4jLevel to WARN and log4jPattern to %-5level - %msg%n in environment.conf
// In containerized set HPCC_JAVA_EMBED_LOG4J_LEVEL and HPCC_JAVA_EMBED_LOG4J_PATTERN env vars

//class=embedded
//class=3rdparty
// We would depend on something like elastic4hpcc in order to capture the logs to test in k8s
//class=3rdpartyservice
//nothor
//noroxie

import java;
import Std.Str;

// Test function that demonstrates various logging levels
STRING testLogging(STRING message) := IMPORT(java, 'javaembedNativeLogging.testLogging:(Ljava/lang/String;)Ljava/lang/String;');

// Get current workunit ID and extract timestamp information
currentWuid := WORKUNIT;
wuidParts := Str.SplitWords(currentWuid, '-');
wuidDatePart := wuidParts[1];
wuidTimePart := wuidParts[2];

// Parse workunit timestamp (assuming format Wyyyymmdd-hhmmss)
// Extract year, month, day, hour, minute, second from wuid
wuidYear := (INTEGER)wuidDatePart[2..5];
wuidMonth := (INTEGER)wuidDatePart[6..7];
wuidDay := (INTEGER)wuidDatePart[8..9];
wuidHour := (INTEGER)wuidTimePart[1..2];
wuidMinute := (INTEGER)wuidTimePart[3..4];
wuidSecond := (INTEGER)wuidTimePart[5..6];

// Create timestamp string for comparison (YYYY-MM-DD HH:MM:SS format)
// Use proper string padding for two-digit numbers
monthStr := IF(wuidMonth < 10, '0' + (STRING)wuidMonth, (STRING)wuidMonth);
dayStr := IF(wuidDay < 10, '0' + (STRING)wuidDay, (STRING)wuidDay);
hourStr := IF(wuidHour < 10, '0' + (STRING)wuidHour, (STRING)wuidHour);
minuteStr := IF(wuidMinute < 10, '0' + (STRING)wuidMinute, (STRING)wuidMinute);
secondStr := IF(wuidSecond < 10, '0' + (STRING)wuidSecond, (STRING)wuidSecond);

wuidTimestamp := (STRING)wuidYear + '-' + monthStr + '-' + dayStr + ' ' + hourStr + ':' + minuteStr + ':' + secondStr;

// Construct log file path
currentDate := (STRING)wuidYear + '_' + monthStr + '_' + dayStr;
logPrefix := '/var/log/HPCCSystems/';
logFilePath := logPrefix + 'myeclagent/eclagent.' + currentDate + '.log';
logCommand := 'cat ' + logFilePath;

// Read log file using PIPE
logLines := PIPE(logCommand, {STRING line}, CSV(SEPARATOR('')));
filteredLogs := logLines(
    LENGTH(TRIM(line)) > 32 AND
    Str.CompareIgnoreCase(line[14..32], wuidTimestamp) >= 0
);
javaembedLogs := filteredLogs(Str.Find(line, 'javaembed: user:') > 0);

// Count logs by log level
LogLevelRec := RECORD
    STRING line;
    STRING logLevel;
END;
javaembedWithLevel := PROJECT(javaembedLogs, TRANSFORM(LogLevelRec,
    SELF.line := LEFT.line,
    SELF.logLevel := MAP(
        // Log4j Log Levels
        Str.Find(LEFT.line, 'FATAL') > 0 => 'FATAL',
        Str.Find(LEFT.line, 'ERROR') > 0 => 'ERROR',
        Str.Find(LEFT.line, 'WARN') > 0 => 'WARN',
        Str.Find(LEFT.line, 'INFO') > 0 => 'INFO',
        Str.Find(LEFT.line, 'DEBUG') > 0 => 'DEBUG',
        Str.Find(LEFT.line, 'TRACE') > 0 => 'TRACE',
        // javaembed plugin statuses
        Str.Find(LEFT.line, 'Enabled Java log redirection') > 0 => 'ENABLED',
        Str.Find(LEFT.line, 'Using custom log4j pattern') > 0 => 'PATTERN',
        Str.Find(LEFT.line, 'Failed to initialize HpccLogHandler') > 0 => 'INITFAIL',
        'UNKNOWN'
    )
));
logCounts := TABLE(javaembedWithLevel, {logLevel; INTEGER count := COUNT(GROUP);}, logLevel);

// Output results
SEQUENTIAL
(
    OUTPUT(testLogging('log4j'), NAMED('JavaLogTest')),
    OUTPUT(COUNT(javaembedLogs), NAMED('JavaembedLogLineCount')),
    OUTPUT(logCounts, NAMED('JavaembedLogLevels'))
);
