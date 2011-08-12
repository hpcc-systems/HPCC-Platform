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

set SENTINEL="roxie.sentinel"

cd /d %1
if errorlevel 1 goto end
call roxievars.bat
if errorlevel 1 goto end
set /a restarts=0

set logfilename=%2

if '%logfilename%' == '' (
   call settime
   set logfilename=%absdt%_%abstm%
)

:begin
start roxie topology=%roxiedir%\RoxieTopology.xml logfile=%logfilename% restarts=%restarts%
pwait roxie
if errorlevel 1 goto end
if not exist %roxiedir%\%SENTINEL% goto end
set /a restarts=%restarts%+1
goto begin
:end
exit 0
