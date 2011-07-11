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

if not exist .\deploy goto :start

xcopy .\deploy .\ /e /v /y 2> nul
if %ERRORLEVEL% NEQ 0 (
   echo Failed to move files from deploy subdirectory to installation directory!
   echo Failed to move files from deploy subdirectory to installation directory! > deploy.log
   sleep 5
   exit 1
)

rd /s /q .\deploy
if %ERRORLEVEL% NEQ 0 (
   echo Failed to remove deploy subdirectory!
   echo Failed to remove deploy subdirectory! > deploy.log
   sleep 5
   exit 1
)

echo Deployment of dali completed successfully! > deploy.log

:start
daserver
exit 0
