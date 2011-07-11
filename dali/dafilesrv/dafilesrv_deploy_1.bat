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

REM Deploy DAFILESRC to remote systems, and install as a system service

REM Handles deployment to single server, intended to be called 
REM by dafilesrv_deploy.bat, which handles setting environment variables
REM and looping through servers. Each server deployment should be
REM handled by an individual start command, to maximize parallelization

SETLOCAL

if "X%1"=="X" goto :whoops
set HOST=%1
echo Deploying to %HOST%
NET USE \\%HOST%\C$ %AUTH%
IF ERRORLEVEL 1 GOTO :whoops

md \\%HOST%\C$\DAFILESRV 2>NUL

echo Attempting to stop service on %HOST%
psexec \\%HOST% %XCMD_AUTH% c:\dafilesrv\dafilesrv -remove
REM No error check here. If this fails, we assume service not running.

FOR %%J in (%DEPLOYFILES%) do (
    XCOPY /F /Y %%J \\%HOST%\C$\%DESTPATH%
    IF ERRORLEVEL 1 GOTO :whoops
)
NET USE \\%HOST%\C$ /d
IF ERRORLEVEL 1 GOTO :whoops


echo Attepting to install DAFILESRV service
psexec \\%HOST% %XCMD_AUTH% c:\dafilesrv\dafilesrv -install
IF ERRORLEVEL 1 GOTO :whoops

echo DAFILESRV_Deploy Completed
exit /b

:whoops
    echo ""
    echo Errors occurred during deploy to %HOST%
    pause Press a key...
