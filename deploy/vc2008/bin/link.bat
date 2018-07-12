@echo off
rem /*##############################################################################
rem 
rem     HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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

setlocal

if NOT "%VS150COMNTOOLS%"=="" (
  rem VS2017 version 15.x
  rem Path and environment variables should already be set up if VsDevCmd.bat has been run
  goto found:
)
if NOT "%VS140COMNTOOLS%"=="" (
  rem VS2015 version 14.x
  call "%VS140COMNTOOLS%vsvars32"
  goto found:
)
if NOT "%VS130COMNTOOLS%"=="" (
  rem VS2013 version 13.x
  call "%VS130COMNTOOLS%vsvars32"
  goto found:
)

rem Older versions do not support c++11 so will not compile the queries

echo Error: Could not locate a supported version of visual studio.

rem Visual Studio 2017 does not install a %VS150COMNTOOLS% variable by default.
if EXIST "%ProgramFiles(x86)%\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" (
  echo Try executing "%ProgramFiles(x86)%\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" first
)

exit 2

:found
link.exe %*

