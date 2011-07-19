@echo off
rem /*##############################################################################
rem 
rem     Copyright (C) 2011 HPCC Systems.
rem 
rem     All rights reserved. This program is free software: you can redistribute it and/or modify
rem     it under the terms of the GNU Affero General Public License as
rem     published by the Free Software Foundation, either version 3 of the
rem     License, or (at your option) any later version.
rem 
rem     This program is distributed in the hope that it will be useful,
rem     but WITHOUT ANY WARRANTY; without even the implied warranty of
rem     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem     GNU Affero General Public License for more details.
rem 
rem     You should have received a copy of the GNU Affero General Public License
rem     along with this program.  If not, see <http://www.gnu.org/licenses/>.
rem ############################################################################## */

if '%regresstgt%'=='' goto novars
rd /s /q %regresstgt%
md %regresstgt% 2>nul

set flags=-P%regresstgt% -legacy -target=thorlcr -fforceGenerate -fdebugNlp=1 -fnoteRecordSizeInGraph -fregressionTest -b -m -S -shared -faddTimingToWorkunit=0 -fshowRecordCountInGraph
set flags=%flags% %regressinclude%

if NOT '%numParallel%'=='' goto multiway
eclcc %flags% %* > out.log
if '%nocompare%'=='1' goto done;
call bc regressNew
goto done
:multiway
for /l %%c in (2,1,%numParallel%) do (
   set ECL_CCLOG=cclog%%c.txt
   start "Regress%%c" /min  eclcc -split %%c%%:%numParallel% %flags% %* 
)

set ECL_CCLOG=cclog1.txt
eclcc -split 1:%numParallel% %flags% %*
set ECL_CCLOG=

pause Press space when done

rem Combine the log file outputs to get a single summary
cd. > %regresstgt%\_batch_.log
for %%f in (%regresstgt%\_batch_.*.log) do type %%f >> %regresstgt%\_batch_.tmp
sort %regresstgt%\_batch_.tmp > %regresstgt%\_batch_.log
del %regresstgt%\_batch_.tmp

if '%nocompare%'=='1' goto done;
call bc regressNew
goto done;

:novars
echo Environment variables regresstgt etc not set up
:done
