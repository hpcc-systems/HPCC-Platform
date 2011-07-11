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

