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
REM Deploy DAFILESRV to remote systems, and install as a system service

REM SETLOCAL

set DEPLOYFILES=dafilesrv.exe,jlib.dll
set DESTPATH=\DAFILESRV

if "X%1"=="X" goto :usage

IF EXIST %1 goto :check_deployables
echo File %1 does not exist
exit /b 1

:check_deployables

FOR %%I in (%DEPLOYFILES%,PSEXEC.EXE) do IF NOT EXIST %%I (
        echo %%I does not exit
        exit /B 1
    )

REM Set user and password here
SET /P DEP_USER="Enter username (%USERNAME%): "
IF "X%DEP_USER%"=="X" GOTO :default_user
    SET /P DEP_PASS="Enter password for %DEP_USER%: "
    SET AUTH=/USER:%DEP_USER% %DEP_PASS%
    SET XCMD_AUTH=-u %DEP_USER% -p %DEP_PASS%
    GOTO :do_deploy
:default_user
    SET AUTH=
    SET XCMD_AUTH=

:do_deploy
FOR /F %%H in (%1) do (
    echo Deploying to %%H
    start "Deploy to %%H" cmd /c dafilesrv_deploy_1.bat %%H
)

echo DAFILESRV_Deploy Completed
echo Successful deployment windows will close automatically
echo Windows that do not close automatically should have error reports
goto :end

:usage
    echo Usage:
    echo DAFILESRV_deploy distribution_file
    
:end
SET AUTH=
SET XCMD_AUTH=
SET DEP_PASS=
SET DEP_USER=
SET DEPLOYFILES=

