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

set flags=-P%regresstgt% -legacy -target=thorlcr -fforceGenerate -fdebugNlp=1 -fnoteRecordSizeInGraph -fregressionTest -b -m -S -shared -faddTimingToWorkunit=0 -fshowRecordCountInGraph
set flags=%flags% %regressinclude%

md %regresstgt% 2>nul
echo %* >> %regresstgt%\stdout.log
eclcc %flags% %* >> %regresstgt%\stdout.log

goto done;

:novars
echo Environment variables regresstgt etc not set up
:done
