@echo off
rem /*##############################################################################
rem
rem     HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
rem
rem     Licensed under the Apache License, Version 2.0 (the "License");
rem     you may not use this file except in compliance with the License.
rem     You may obtain a copy of the License at
rem
rem        http://www.apache.org/licenses/LICENSE-2.0
rem
rem     Unless required by applicable law or agreed to in writing, software
rem     distributed under the License is distributed on an "AS IS" BASIS,
rem     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem     See the License for the specific language governing permissions and
rem     limitations under the License.
rem ############################################################################## */

setlocal enableextensions enabledelayedexpansion

if '%regresstgt%'=='' goto novars
rd /s /q %regresstgt%
md %regresstgt% 2>nul

set flags=-P%regresstgt% -legacy -target=thorlcr -fforceGenerate -fdebugNlp=1 -fnoteRecordSizeInGraph -fregressionTest -b -m -S -shared -faddTimingToWorkunit=0 -fshowRecordCountInGraph
set flags=%flags% %regressinclude% -fshowMetaInGraph -fspanMultipleCpp-

if NOT '%regressProcesses%'=='' goto multiway
eclcc %flags% %* > out.log
goto compare;

:multiway
for /l %%c in (2,1,%regressProcesses%) do (
   set ECL_CCLOG=cclog%%c.txt
   start "Regress%%c" /min  eclcc -split %%c%%:%regressProcesses% %flags% %*
)

set ECL_CCLOG=cclog1.txt
eclcc -split 1:%regressProcesses% %flags% %*
set ECL_CCLOG=

pause Press space when done

rem Combine the log file outputs to get a single summary
cd. > %regresstgt%\_batch_.log
for %%f in (%regresstgt%\_batch_.*.log) do type %%f >> %regresstgt%\_batch_.tmp
sort %regresstgt%\_batch_.tmp > %regresstgt%\_batch_.log
del %regresstgt%\_batch_.tmp

:compare
if EXIST %~dp0\rcompare.bat. call %~dp0\rcompare
goto done;

:novars
echo Environment variables regresstgt etc not set up
:done
