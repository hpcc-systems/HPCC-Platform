@echo off
rem /*##############################################################################
rem 
rem     HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
rem 
rem     Licensed under the Apache License, Version 2.0 (the "License");
rem     you may not use this file except in compliance with the License.
rem     You may obtain a copy of the License at
rem 
rem        http://www.apache.org/licenses/LICENSE-2.0
rem 
rem     Unless required by applicable law or agreed to in writing, software
rem     distributed under the License is distributed on an "AS IS" BASIS,
rem     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem     See the License for the specific language governing permissions and
rem     limitations under the License.
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
