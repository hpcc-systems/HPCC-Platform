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

setlocal

if '%1'=='add' if '%2' NEQ '' goto add
if '%1'=='remove' goto remove
goto syntax

:add
set /p resp=Are you sure you want to secure Configenv using '%2'? [y/n] 
if '%resp%'=='y' sdsfix . set "/Environment/@wsConfigUrl" "%2"
goto end

:remove
set /p resp=Are you sure you want to remove security from Configenv? [y/n] 
if '%resp%'=='y' sdsfix . set "/Environment/@wsConfigUrl" ""
goto end

:syntax
echo This batch file associates a URL of an ECL Watch service with
echo the super computer environment maintained in the Dali server.
echo The deployment tool (Configenv) then enforces user-level 
echo authentication for accessing the environment using an LDAP 
echo server resource 'ConfigAccess'.

echo syntax: 
echo secure_configenv add -URL-
echo or
echo secure_configenv remove
echo URL is of the form http://host_or_ip_address[:port]
echo or
echo https://ip_address[:port]

:end