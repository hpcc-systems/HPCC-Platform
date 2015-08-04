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

if '%regresstgt%'=='' goto novars

set flags=-P%regresstgt% -legacy -target=thorlcr -fforceGenerate -fdebugNlp=1 -fnoteRecordSizeInGraph -fregressionTest -b -m -S -shared -faddTimingToWorkunit=0 -fshowRecordCountInGraph
set flags=%flags% %regressinclude% -fshowMetaInGraph -fspanMultipleCpp-

md %regresstgt% 2>nul
echo %* >> %regresstgt%\stdout.log
eclcc %flags% %* >> %regresstgt%\stdout.log

goto done;

:novars
echo Environment variables regresstgt etc not set up
:done
