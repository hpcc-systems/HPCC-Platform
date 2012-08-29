@echo off
rem /*##############################################################################
rem 
rem     HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

if "%5" == "" goto 1
for %%x in (%0) do set deploydir=%%~dpsx
for %%x in (%deploydir%) do set deploydir=%%~dpsx
mkdir %1 2> nul
pushd %1
del %5 2> nul
rem %deploydir%keypatch -o %4 %3 %2 2>%5.log
if not errorlevel 1 goto 2
echo err=%ERRORLEVEL% >>%5.log
2:
ren %5.log %5
popd
goto 999
:1
echo usage: %0 workdir newkeyfile oldkeyfile patchfile flagfile
:999

