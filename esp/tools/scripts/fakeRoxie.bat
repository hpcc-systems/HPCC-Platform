@echo off
rem /*##############################################################################
rem 
rem     HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
    echo This batch file generates cumulative output for a given query ^(commonly
    echo known as 'fake roxie'^).  It first fetches an XML schema for that query,
    echo generating a dummy request.   It then sends that dummy request to Roxie
    echo to fetch XML schema for the response, which is embedded in the response.
    rem
    echo Instructions:
    rem
    echo 1. set path to include directory where soapplus resides
    echo 2. change parameters as set in the set instructions below
    exit /b 0
)

setlocal
path=binaries;%path%

rem set the following parameters to suppress their prompts 
rem
set url=http://10.173.72.218:8522
set userid=
set password=
set module=Doxie
set query=Comprehensive_Report_Service

if "%url%" == "" set /p url=Enter URL for WsECL (as http[s]://<host>[:<port>]):
if "%userid%" == "" set /p userid=Enter UserId:
if "%password%" == "" set /p password=Enter Password (Warning: Password is echoed back!):
set /p x=Enter ECL Module Name[%module%]:
if not "%x%" == "" set module=%x%
set x=
set /p x=Enter ECL Query Name[%query%]:
if not "%x%" == "" set query=%x%
set x=

rem you can raise the following tracing level upto 10 (see soapplus -h for more info)
set tracing=0

rem make url2 like http://userid:password@ip_port/module/module.query
set ip_port=%url:*//=%
set fifth_char=%url:~4,1%
set protocol=http
if not "%fifth_char%" == ":" set protocol=%protocol%s
set url2=%protocol%://%userid%:%password%@%ip_port%/%module%/%module%.%query%

rem change directory to that of this batch file
for %%D in (%0) do cd /d %%~dpD

echo Fetching XML schema for the request and generating a request instance ...
soapplus -xsd %url2%?xsd -m %module%.%query%Request -d %tracing% -g -gs -wiz -gf _request.xml

echo Running a dummy request to get a sample response with embedded XML schema ...
soapplus -url %url2%?format_=xml -d %tracing% -w -o _tmp

move /Y _tmp\response_content\get.txt _response.xml
rd /s /q _tmp
mkdir fake\%module% 2> nul
echo Filling in missing XML elements in response using embedded XML schema and generating fake\%module%\%query%.xml ...
soapplus -r -ra -g -xsd _response.xml -i _response.xml -m %query%Response -wiz -d %tracing% -gf fake\%module%\%query%.xml

del _request.xml _response.xml 2> nul
