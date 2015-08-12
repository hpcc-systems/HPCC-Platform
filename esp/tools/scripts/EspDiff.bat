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


if "%1" == "-h" (
    echo usage: EspDiff queryName  example: EspDiff AssetReport
    echo        EspDiff -h
    echo ----------------------------------------------------------            
    echo coding instructions:
    echo 1. change the path to include soapplus.exe and examxml.lnk 
    echo    ^(shortcut to examxml^).
    echo 2. define esp1 and esp2 as http[s]://userid:password@url:port/
    echo 3. define ver1 and ver2
    echo 4. define param1 and param2 to include any extra params 
    echo    escaping out ^& character as shown below
    echo 5. put SOAP test files in EspDiff.Requests
    echo 6. define dir1 and dir2 as output dirs for esp1 and esp2 tests
    echo --------------------------------------------------------------
    exit /b 0
)

set query=%1

setlocal enabledelayedexpansion
set path=%cd%\binaries;%path%;C:\Program Files\ExamXML;

set esp1=http://userid:password@url:port/
set esp2=%esp1%

set ver1=1.20
set ver2=9.99

rem set param1=
rem set param2=^^^&internal

set requestDir=EspDiff.Requests

set dir1=EspDiff.Responses\esp1
set dir2=EspDiff.Responses\esp2

set service=WsAccurint
set validate=-v

echo Running requests in %dir1% url: %esp1%/%service%?ver_=%ver1%%param1%
mkdir %dir1% 2> nul
soapplus -c -d 0 -url %esp1%/%service%/?ver_=%ver1%%param1% -i %requestDir%\%query%.xml -o %dir1% -w

echo Running requests in %dir2% ...
mkdir %dir2% 2> nul
soapplus -c -d 0 -url %esp2%/%service%/?ver_=%ver2%%param2% -i %requestDir%\%query%.xml -o %dir2% -w

for %%i in (%requestDir%\%query%.xml) do (
    set file=response_content\%%~nxi
        echo %file%
    examxml.exe %dir1%\!file! %dir2%\!file!
)
