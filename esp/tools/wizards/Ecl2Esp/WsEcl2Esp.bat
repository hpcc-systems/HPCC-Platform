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

setlocal
rem set the following parameters to suppress their prompts 
rem set url=
rem set userid=
rem set password=
rem set module=
rem set query=
rem set method=

if "%url%" == "" set /p url=Enter URL for WsECL (as http[s]://^<host^>[:^<port^>]):
if "%userid%" == "" set /p userid=Enter UserId:
if "%password%" == "" set /p password=Enter Password (Warning: Password is echoed back!):
if "%module%" == "" set /p module=Enter ECL Module Name:
if "%query%" == "" set /p query=Enter ECL Query Name:

if "%method%" == "" set /p method=Enter ESP Method Name [%query%]:
if "%method%" == "" set method=%query%

rem you can raise the following tracing level upto 10 (see soapplus -h for more info)
set tracing=0

rem make url2 like http://userid:password@ip_port/module/module.query
set ip_port=%url:*//=%
set fifth_char=%url:~4,1%
set protocol=http
if not "%fifth_char%" == ":" set protocol=%protocol%s
set url2=%protocol%://%userid%:%password%@%ip_port%/%module%/%module%.%query%

echo Processing %method% ...
rem change directory to that of this batch file
for %%D in (%0) do cd /d %%~dpD
set path=%path%;..\..\..\..\bin\debug
md %method% 2> nul
echo Fetching XML schema for the request and generating a request instance %method%Request.xml ...
soapplus -xsd %url2%?xsd -m %module%.%query%Request -d %tracing% -g -gs -wiz -gf %method%\%method%Request.xml

echo Running a dummy request to get a sample response with embedded XML schema in %method%ResponseFull.xml ...
soapplus -url %url2%?format_=xml -d %tracing% -w -o _tmp

move /Y _tmp\response_content\get.txt %method%\%method%ResponseFull.xml
rd /s /q _tmp

rem print the available Datasets in the response
echo The following Datasets are returned in the query response.
grep "Dataset name=" %method%\%method%ResponseFull.xml | perl -ne "if(m/<Dataset name='(.*)'>/){print \"$1\n\"}"

rem the following defines the name of the dataset in the ECL response with results
rem Note that most queries use the name 'Results'
if "%dataset%" == "" set /p dataset=Enter Dataset name in ECL Response [Results]:
if "%dataset%" == "" set dataset=Results

echo Filling in missing XML elements in %method%ResponseFull.xml using embedded XML schema and generating %method%Response.xml ...
soapplus -r -ra -g -xsd %method%\%method%ResponseFull.xml -i %method%\%method%ResponseFull.xml -m %method%Response -wiz -d %tracing% -gf %method%\%method%Response.xml

rem generate CustomNames.xml from CustomNames.txt
..\..\..\..\tools\misc\awk -f ini2xml.awk CustomNames.txt > CustomNames.xml

set xslfile=xml2scm.xsl
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.scm
echo Generating request  interface as %method%Request.scm ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.scm
echo Generating response interface as %method%Response.scm (may take a few minutes) ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p responseDataset '"%dataset%"' -o %outfile% %infile% %xslfile%

set xslfile=xml2qxslt.xsl
set infile=%method%\%method%Request.xml
set outfile=%method%\%method%Request.cpp
echo Generating C++ code for request  as %method%Request.cpp ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 0 -o %outfile% %infile% %xslfile%

set infile=%method%\%method%Response.xml
set outfile=%method%\%method%Response.cpp
echo Generating C++ code for response as %method%Response.cpp (may take a few minutes) ...
..\..\..\..\system\xalan\xalan -p method '%method%' -p response 1 -p responseDataset '"%dataset%"'  -o %outfile% %infile% %xslfile%


cd %method%
echo Merging %method%Request.scm and %method%Response.scm into wsm_%method%.scm
copy /Y %method%Request.scm wsm_%method%.scm > nul
type %method%Response.scm >> wsm_%method%.scm
del %method%Re*.scm 2> nul

echo Merging %method%Request.cpp and %method%Response.cpp into rx_%method%.cpp
copy /Y %method%Request.cpp rx_%method%.cpp > nul
type %method%Response.cpp >> rx_%method%.cpp
del %method%Re*.cpp 2> nul

echo ESP code generation complete!

if not "%WsEcl2EspPause%" == "no" pause
