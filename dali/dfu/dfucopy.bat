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

SETLOCAL
if "%5"=="" goto error
set _options=
if "%6"=="" goto nosize
set options=/nosplit /connect:%6
:nosize
dfu daliservers=%1 savemap %2 temp.map
dfu daliservers=%3 import temp.map %4 ::%5 /crc /replicate /pull %options%
goto end
:error
echo "Usage DFUCOPY <src_dali> <src_lfn> <dst_dali> <dst_lfn> <dst_group> [size]"
:end
