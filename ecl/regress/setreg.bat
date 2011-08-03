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
rem
rem Rename this file, and then modify the following values below to match your local system
rem
rem The directory you wanr the target results to be written to
set regresstgt=d:\regression

rem The directory that contains the "correct" results from a previous run
set regresskey=c:\regression

rem Any includes which need to be passed to eclcc
set regressinclude=-Ic:\hpcc\HPCC-Platform\ecl\regress\modules
